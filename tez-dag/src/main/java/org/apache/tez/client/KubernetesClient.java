package org.apache.tez.client;


import com.google.common.collect.ImmutableMap;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1ConfigMapVolumeSourceBuilder;
import io.kubernetes.client.models.V1EnvVarBuilder;
import io.kubernetes.client.models.V1PersistentVolumeClaimVolumeSourceBuilder;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodBuilder;
import io.kubernetes.client.models.V1PodStatus;
import io.kubernetes.client.models.V1VolumeBuilder;
import io.kubernetes.client.util.Config;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.apache.commons.codec.binary.Base64.*;

public class KubernetesClient extends FrameworkClient {

    private CoreV1Api api;
    private TezConfiguration conf;
    private boolean isSession;
    private boolean isRunning;

    @Override
    public void init(TezConfiguration tezConf) {
        this.conf = tezConf;
        isSession = tezConf.getBoolean(TezConfiguration.TEZ_AM_SESSION_MODE,
                TezConfiguration.TEZ_AM_SESSION_MODE_DEFAULT);
    }

    @Override
    public void start() {
        try {
            api = new CoreV1Api(Config.fromCluster());
            api.getApiClient().setDebugging(true);
            isRunning = true;
        } catch (IOException exception) {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void stop() {
        isRunning = false;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public YarnClientApplication createApplication() throws YarnException, IOException {
        ApplicationSubmissionContext context = Records.newRecord(ApplicationSubmissionContext.class);
        ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 0);
        context.setApplicationId(appId);
        GetNewApplicationResponse response = Records.newRecord(GetNewApplicationResponse.class);
        response.setApplicationId(appId);
        return new YarnClientApplication(response, context);
    }

    @Override
    public ApplicationId submitApplication(ApplicationSubmissionContext appSubmissionContext) throws YarnException, IOException, TezException {
        ApplicationId appId = appSubmissionContext.getApplicationId();
        try {
            startDAGAppMaster(appSubmissionContext);
        } catch (Exception e) {
            throw new TezException("DAGAppMaster got an error during initialization", e);
        }
        return appId;
    }

    private void startDAGAppMaster(ApplicationSubmissionContext appSubmissionContext) throws ApiException {
        ByteBuffer tokens = Base64.getEncoder().encode(appSubmissionContext.getAMContainerSpec().getTokens());
        Map<String, LocalResource> localResources = appSubmissionContext.getAMContainerSpec().getLocalResources();
        String localResourcesString = localResources.values().stream().map(res -> res.getResource().getFile()).collect(Collectors.joining(":"));
        ContainerId containerId = ContainerId.newContainerId(ApplicationAttemptId.newInstance(appSubmissionContext.getApplicationId(), 0), 1);

        int rpcPort = conf.getInt(TezConfiguration.TEZ_AM_RPC_PORT,
                TezConfiguration.TEZ_AM_RPC_PORT_DEFAULT);
        V1Pod pod = new V1PodBuilder()
                .withNewMetadata()
                .withName("friction-reduce-" + appSubmissionContext.getApplicationId().getId())
                .withLabels(ImmutableMap.<String, String>builder().put("name", "friction-reduce").build())
                .endMetadata()
                .withNewSpec()
                .withHostname("friction-reduce-" + appSubmissionContext.getApplicationId().getId())
                .withSubdomain("friction-reduce")
                .addNewContainer()
                .addNewPort()
                .withContainerPort(rpcPort)
                .endPort()
                .withName("dag-app-master")
                .withImage("data-lake/hive-am:4.0.0_0")
                .withImagePullPolicy("Never")
                .addNewEnv()
                .withName("LOCAL_RESOURCES")
                .withValue(localResourcesString)
                .endEnv()
                .addNewEnv()
                .withName("CONTAINER_ID")
                .withValue(containerId.toString())
                .endEnv()
                .addNewEnv()
                .withName("HADOOP_TOKEN_BASE64")
                .withValue(StandardCharsets.US_ASCII.decode(tokens).toString())
                .endEnv()
                .addNewVolumeMount()
                .withName("krb5-conf")
                .withMountPath("/etc/krb5.conf")
                .withSubPath("krb5.conf")
                .endVolumeMount()
                .addNewVolumeMount()
                .withName("keytabs-pv")
                .withMountPath("/etc/security/keytabs")
                .endVolumeMount()
                .endContainer()
                .withVolumes(
                        new V1VolumeBuilder()
                                .withName("krb5-conf")
                                .withConfigMap(new V1ConfigMapVolumeSourceBuilder().withName("krb5-conf").build())
                                .build(),
                        new V1VolumeBuilder()
                                .withName("keytabs-pv")
                                .withPersistentVolumeClaim(new V1PersistentVolumeClaimVolumeSourceBuilder().withClaimName("keytabs-pv").build())
                                .build())
                .endSpec()
                .build();

        api.createNamespacedPod("hdfs", pod, null, null, null);

        while (getApplicationReport(appSubmissionContext.getApplicationId()).getYarnApplicationState() != YarnApplicationState.RUNNING) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void killApplication(ApplicationId appId) throws YarnException, IOException {

    }

    @Override
    public ApplicationReport getApplicationReport(ApplicationId appId) {
        try {
            int rpcPort = conf.getInt(TezConfiguration.TEZ_AM_RPC_PORT,
                    TezConfiguration.TEZ_AM_RPC_PORT_DEFAULT);
            V1Pod pod = api.readNamespacedPodStatus("friction-reduce-" + appId.getId(), "hdfs", null);
            ApplicationReport report = Records.newRecord(ApplicationReport.class);
            report.setApplicationId(appId);
            report.setCurrentApplicationAttemptId(ApplicationAttemptId.newInstance(appId, 0));
            report.setUser("hive");
            report.setName("noname - should be dag");
            report.setStartTime(System.currentTimeMillis());
            report.setHost(pod.getStatus().getPodIP());
            report.setRpcPort(rpcPort);
            ClientToAMTokenIdentifier clientToAMTokenIdentifier = new ClientToAMTokenIdentifier(report.getCurrentApplicationAttemptId(), "hive");
            byte[] masterKey = decodeBase64("H94Te5RPoLc=");
            ClientToAMTokenSecretManager sm = new ClientToAMTokenSecretManager(report.getCurrentApplicationAttemptId(), null);
            sm.setMasterKey(masterKey);
            try {
                byte[] password = sm.retrievePassword(clientToAMTokenIdentifier);
                byte[] secret =// encodeBase64(
                        clientToAMTokenIdentifier.getProto().toByteArray();//);
                System.out.println("ATTA: " + new String(secret, StandardCharsets.UTF_8));
                Token token = Token.newInstance(secret,
                        ClientToAMTokenIdentifier.KIND_NAME.toString(),
                        password,
                        pod.getStatus().getPodIP() + ":" + rpcPort);

                report.setClientToAMToken(token);
            } catch (SecretManager.InvalidToken invalidToken) {
                invalidToken.printStackTrace();
            }
            report.setYarnApplicationState(toYarnApplicationState(pod.getStatus()));
            report.setFinalApplicationStatus(FinalApplicationStatus.UNDEFINED);


            //List<String> diagnostics = dagAppMaster.getDiagnostics();
            //if (diagnostics != null) {
            //    report.setDiagnostics(diagnostics.toString());
            //}
            report.setTrackingUrl("N/A");
            report.setFinishTime(0);
            report.setApplicationResourceUsageReport(null);
            report.setOriginalTrackingUrl("N/A");
            report.setProgress(0);
            report.setAMRMToken(null);
            return report;
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
    }

    private YarnApplicationState toYarnApplicationState(V1PodStatus status) {
        switch (status.getPhase()) {
            case "Pending":
                return YarnApplicationState.ACCEPTED;
            case "Running":
                return YarnApplicationState.RUNNING;
            case "Failed":
                return YarnApplicationState.FAILED;
            default:
                return YarnApplicationState.KILLED;
        }
    }

    @Override
    public boolean isRunning() throws IOException {
        return isRunning;
    }
}

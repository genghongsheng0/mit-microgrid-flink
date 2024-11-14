//package com.missiongroup.mitmicrogridflink.job;
//
//import org.apache.flink.api.common.JobID;
//import org.apache.flink.client.program.ClusterClient;
//import org.apache.flink.client.program.ClusterClientProvider;
//import org.apache.flink.runtime.client.JobSubmissionException;
//import org.apache.flink.runtime.jobgraph.JobGraph;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Controller;
//import org.springframework.web.bind.annotation.*;
//
//import java.util.concurrent.ExecutionException;
//
//@RestController
//@RequestMapping("/flink")
//public class FlinkJobController {
//
//    @Autowired
//    private ClusterClientProvider<?> clusterClientProvider;
//
//    @PostMapping("/submit")
//    private String submit(@RequestBody String sqls) {
//        JobGraph jobGraph = FlinkJob.createJobGraph(sqls);
//        try (ClusterClient<?> client = clusterClientProvider.getClusterClient()) {
//            JobID jobID = client.submitJob(jobGraph).get();
//            return "Job submitted successfully with ID: " + jobID.toString();
//        } catch (InterruptedException | ExecutionException e) {
//            e.printStackTrace();
//            return "Job submission failed: " + e.getMessage();
//        }
//
//    }
//}

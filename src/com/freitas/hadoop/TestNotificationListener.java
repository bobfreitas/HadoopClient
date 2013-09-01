package com.freitas.hadoop;

import java.util.HashMap;
import java.util.Map;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestNotificationListener implements
		PigProgressNotificationListener {

private static final Logger log = LoggerFactory.getLogger(TestNotificationListener.class);
	
	private Map<String, int[]> numMap = new HashMap<String, int[]>();
    
    private static final int JobsToLaunch = 0;
    private static final int JobsSubmitted = 1;
    private static final int JobStarted = 2;
    private static final int JobFinished = 3;

    @Override
    public void initialPlanNotification(String id, MROperPlan plan) {
    	log.error("initialPlanNotification-id: " + id + " planNodes: " + plan.getKeys().size());
    }

    @Override
    public void launchStartedNotification(String id, int numJobsToLaunch) {
    	log.error("launchStartedNotification-id: " + id + " numJobsToLaunch: " + numJobsToLaunch);
        int[] nums = new int[4];
        numMap.put(id, nums);
        nums[JobsToLaunch] = numJobsToLaunch;
    }

    @Override
    public void jobFailedNotification(String id, JobStats jobStats) {
    	log.error("jobFailedNotification-id: " + id + " job failed: " + jobStats.getJobId());
    }

    @Override
    public void jobFinishedNotification(String id, JobStats jobStats) {
    	log.error("jobFinishedNotification-id: " + id + " job finished: " + jobStats.getJobId()); 
        int[] nums = numMap.get(id);
        nums[JobFinished]++;
    }

    @Override
    public void jobStartedNotification(String id, String assignedJobId) {
    	log.error("jobStartedNotification-id: " + id + " job started: " + assignedJobId);   
        int[] nums = numMap.get(id);
        nums[JobStarted]++;
    }

    @Override
    public void jobsSubmittedNotification(String id, int numJobsSubmitted) {
    	log.error("jobsSubmittedNotification-id: " + id + " jobs submitted: " + numJobsSubmitted);
        int[] nums = numMap.get(id);
        nums[JobsSubmitted] += numJobsSubmitted;
    }

    @Override
    public void launchCompletedNotification(String id, int numJobsSucceeded) {
    	log.error("launchCompletedNotification-id: " + id + " numJobsSucceeded: " + numJobsSucceeded);
//        System.out.println("");
//        int[] nums = numMap.get(id);
//        assertEquals(nums[JobsToLaunch], numJobsSucceeded);
//        assertEquals(nums[JobsSubmitted], numJobsSucceeded);
//        assertEquals(nums[JobStarted], numJobsSucceeded);
//        assertEquals(nums[JobFinished], numJobsSucceeded);
    }

    @Override
    public void outputCompletedNotification(String id, OutputStats outputStats) {
    	log.error("outputCompletedNotification-id: " + id + " output done: " + outputStats.getLocation());
    }

    @Override
    public void progressUpdatedNotification(String id, int progress) {
    	log.error("progressUpdatedNotification-id: " + id + " progress: " + progress + "%");
    }

}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uta.ak.usttmp.dmcore.controller;

import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;
import uta.ak.usttmp.common.service.TopicEvolutionService;
import uta.ak.usttmp.common.service.TopicMiningService;
import uta.ak.usttmp.dmcore.service.MiningTaskService;

/**
 *
 * @author zhangcong
 */
@Controller
public class TestController {
    
    @Autowired
    private TopicMiningService topicMiningService;
    @Autowired
    private TopicEvolutionService topicEvolutionService;
    @Autowired
    private MiningTaskService miningTaskService;
    @Autowired
    private Scheduler quartzScheduler;
    
    @RequestMapping("/triggerQuartzJob")
    public ModelAndView triggerQuartzJob(String jobname,
                                         String jobgroup) throws SchedulerException {
 
        JobKey jobKey = JobKey.jobKey(jobname, jobgroup);
        quartzScheduler.triggerJob(jobKey);
        
        ModelAndView mav=new ModelAndView("blank");
        return mav;
    }
    
    @RequestMapping("/deleteQuartzJob")
    public ModelAndView deleteQuartzJob(String jobname,
                                        String jobgroup) throws SchedulerException {
 
        JobKey jobKey = JobKey.jobKey(jobname, jobgroup);
        quartzScheduler.deleteJob(jobKey);
        
        ModelAndView mav=new ModelAndView("blank");
        return mav;
    }

    /**
     * @return the topicMiningService
     */
    public TopicMiningService getTopicMiningService() {
        return topicMiningService;
    }

    /**
     * @param topicMiningService the topicMiningService to set
     */
    public void setTopicMiningService(TopicMiningService topicMiningService) {
        this.topicMiningService = topicMiningService;
    }

    /**
     * @return the topicEvolutionService
     */
    public TopicEvolutionService getTopicEvolutionService() {
        return topicEvolutionService;
    }

    /**
     * @param topicEvolutionService the topicEvolutionService to set
     */
    public void setTopicEvolutionService(TopicEvolutionService topicEvolutionService) {
        this.topicEvolutionService = topicEvolutionService;
    }

    /**
     * @return the miningTaskService
     */
    public MiningTaskService getMiningTaskService() {
        return miningTaskService;
    }

    /**
     * @param miningTaskService the miningTaskService to set
     */
    public void setMiningTaskService(MiningTaskService miningTaskService) {
        this.miningTaskService = miningTaskService;
    }

    /**
     * @return the quartzScheduler
     */
    public Scheduler getQuartzScheduler() {
        return quartzScheduler;
    }

    /**
     * @param quartzScheduler the quartzScheduler to set
     */
    public void setQuartzScheduler(Scheduler quartzScheduler) {
        this.quartzScheduler = quartzScheduler;
    }
}

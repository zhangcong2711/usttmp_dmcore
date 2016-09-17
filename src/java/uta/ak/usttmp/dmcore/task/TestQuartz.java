/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uta.ak.usttmp.dmcore.task;

import org.quartz.Job;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
//import uta.ak.usttmp.common.service.TopicEvolutionService;
import uta.ak.usttmp.common.service.TopicMiningService;
import uta.ak.usttmp.dmcore.service.MiningTaskService;

/**
 *
 * @author zhangcong
 */
public class TestQuartz implements ApplicationContextAware{
    
    // Spring ApplicationContext
    private ApplicationContext applicationContext;
    
    private TopicMiningService topicMiningService;
//    private TopicEvolutionService topicEvolutionService;
    private MiningTaskService miningTaskService;
    
    public static void main (String[] args) throws java.io.IOException {
        
        new TestQuartz().doTest();
    }
    
    public void doTest(){
        topicMiningService=(TopicMiningService) 
                                applicationContext
                                    .getBean("topicMiningService");
//        topicEvolutionService=(TopicEvolutionService) 
//                                applicationContext
//                                    .getBean("topicEvolutionService");
        miningTaskService=(MiningTaskService)
                              applicationContext
                                    .getBean("miningTaskService");
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext=applicationContext;
    }
    
}

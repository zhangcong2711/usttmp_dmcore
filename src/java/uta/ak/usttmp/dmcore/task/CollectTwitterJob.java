/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uta.ak.usttmp.dmcore.task;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.PersistJobDataAfterExecution;

/**
 *
 * @author zhangcong
 */
@DisallowConcurrentExecution
@PersistJobDataAfterExecution
public class CollectTwitterJob implements Job{

    private static final Logger logger = Logger.getLogger(CollectTwitterJob.class);  
    
    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {
        
        try{
            
            String tagName=(String) jec.getMergedJobDataMap().get("tagName");
            
            SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
        
            Date nowDt=new Date();

            Calendar cal = Calendar.getInstance();
            cal.setTime(nowDt);
            cal.add(Calendar.HOUR_OF_DAY, -24);
            Date yesterdayDt=cal.getTime();

            CollectTweets ct = new CollectTweets();
            ct.collectTweetsByFileList(
                                       format1.format(yesterdayDt),
                                       format1.format(nowDt),
                                       tagName);
            
        }catch(Exception e){
            
            JobExecutionException je =new JobExecutionException("Exception when collect twitter daily. ");
            je.setStackTrace(e.getStackTrace());
            
            throw je;
            
        }
        
        
    }
    
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uta.ak.usttmp.dmcore.task;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;  
import java.util.Calendar;
import java.util.Date;  
import java.util.List;
import java.util.logging.Level;
import javax.sql.DataSource;
  
import org.apache.log4j.Logger;  
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;  
import org.quartz.JobExecutionContext;  
import org.quartz.JobExecutionException; 
import org.quartz.PersistJobDataAfterExecution;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import uta.ak.usttmp.common.dao.UsttmpDaoSupport;
import uta.ak.usttmp.common.model.MiningTask;
import uta.ak.usttmp.common.dao.mapper.MiningTaskRowMapper;
import uta.ak.usttmp.common.exception.UsttmpProcessException;
import uta.ak.usttmp.common.model.LdaMiningTask;
import uta.ak.usttmp.common.model.Topic;
import uta.ak.usttmp.common.service.TopicEvolutionService;
import uta.ak.usttmp.common.service.TopicMiningService;
import uta.ak.usttmp.dmcore.service.MiningTaskService;

/**
 *
 * @author zhangcong
 */
@DisallowConcurrentExecution
@PersistJobDataAfterExecution
public class QuartzMiningJob implements Job{
    
    private static final Logger logger = Logger.getLogger(QuartzMiningJob.class);  
    
    // Spring ApplicationContext
    private ApplicationContext applicationContext;
    
    private long miningTaskId;
    private String jobName;
    
    
    private TopicMiningService topicMiningService;
    private TopicEvolutionService topicEvolutionService;
    private MiningTaskService miningTaskService;

    @Override
    public void execute(JobExecutionContext jec){
        
        String updateSql="UPDATE `c_miningtask`  " +
                            "SET `qrtz_job_name` = ?,  " +
                            " `qrtz_job_exec_count` = ?,  " +
                            " `status` = ?  " +
                            "WHERE  " +
                            "	`mme_eid` = ?;";
        
        

        DataSource ds =null;
        JdbcTemplate jt=null;
        
        int nextExecCount=0;
        
        try{
            
            applicationContext= new ClassPathXmlApplicationContext("applicationContext.xml");
//            applicationContext = new FileSystemXmlApplicationContext("web/WEB-INF/applicationContext.xml");
            ds = 
                (DataSource) 
                applicationContext.getBean("dataSource");
            jt=new JdbcTemplate(ds);
            
            topicMiningService=(TopicMiningService) 
                                applicationContext
                                    .getBean("topicMiningService");
            topicEvolutionService=(TopicEvolutionService) 
                                    applicationContext
                                        .getBean("topicEvolutionService");
            miningTaskService=(MiningTaskService)
                                  applicationContext
                                        .getBean("miningTaskService");
            
            miningTaskId=Long.parseLong((String) 
                                         jec.getMergedJobDataMap()
                                             .get("miningTaskId"));
            jobName=(String) jec.getMergedJobDataMap().get("jobName");
            
            //Load Miningtask
            String querySql="select * from c_miningtask where mme_eid=?";
            LdaMiningTask mt=(LdaMiningTask) jt.queryForObject(querySql, 
                              new Object[]{miningTaskId}, 
                              new MiningTaskRowMapper());
            
            nextExecCount = mt.getQrtzJobExecCount()+1;
            
            //Calculate the time period
            Calendar cal = Calendar.getInstance();
            cal.setTime(mt.getStartTime());
            cal.add(Calendar.HOUR_OF_DAY,
                    mt.getMiningInterval() * mt.getQrtzJobExecCount());
            Date startTime=cal.getTime();
            cal.setTime(mt.getStartTime());
            cal.add(Calendar.HOUR_OF_DAY,
                    mt.getMiningInterval() * nextExecCount);
            Date endTime=cal.getTime();
            
            //Mining Process
            List<Topic> topicList=topicMiningService
                                      .generateTopics(miningTaskId, 
                                                      startTime,
                                                      endTime,
                                                      mt.getTag(), 
                                                      nextExecCount,
                                                      mt.getTopicNum(), 
                                                      mt.getKeywordNum(), 
                                                      mt.getAlpha(), 
                                                      mt.getBeta(), 
                                                      mt.getRemark());
            
            if(nextExecCount>1){
                if(null!=topicList && !topicList.isEmpty()){
                    topicEvolutionService.
                        calculateTopicEvolutionRelationships(miningTaskId, 
                                                             nextExecCount-1, 
                                                             nextExecCount);
                }
            }
            
            jt.update(updateSql, 
                      jobName,
                      nextExecCount,
                      MiningTask.STATUS_RUNNING,
                      miningTaskId);
            
        }catch(UsttmpProcessException e){
            
            if(UsttmpProcessException
                   .TYPE_CALC_EVO_RELA_EXCEPTION.equals(e.getMessage())){
                
                jt.update(updateSql, 
                          jobName,
                          nextExecCount,
                          MiningTask.STATUS_RUNNING,
                          miningTaskId);
                
            }else{
                
            }
            
            e.printStackTrace();
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());
            //log exception table
            miningTaskService.logMiningTask(MiningTaskService.LOG_TYPE_EXCEPTION, 
                                            miningTaskId, 
                                            errors.toString());
            
            
        }
        catch(Exception e){
            
            e.printStackTrace();
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());
            //log exception table
            miningTaskService.logMiningTask(MiningTaskService.LOG_TYPE_EXCEPTION, 
                                            miningTaskId, 
                                            errors.toString());
            
        }finally{
            
            
        }
    }

//    @Override
//    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
//        this.applicationContext=applicationContext;
//    }

//    /**
//     * @return the miningTaskId
//     */
//    public String getMiningTaskId() {
//        return String.valueOf(miningTaskId);
//    }
//
//    /**
//     * @param miningTaskId the miningTaskId to set
//     */
//    public void setMiningTaskId(String miningTaskId) {
//        this.miningTaskId = Long.parseLong(miningTaskId);
//    }

//    /**
//     * @return the topicMiningService
//     */
//    public TopicMiningService getTopicMiningService() {
//        return topicMiningService;
//    }
//
//    /**
//     * @param topicMiningService the topicMiningService to set
//     */
//    public void setTopicMiningService(TopicMiningService topicMiningService) {
//        this.topicMiningService = topicMiningService;
//    }
//
//    /**
//     * @return the topicEvolutionService
//     */
//    public TopicEvolutionService getTopicEvolutionService() {
//        return topicEvolutionService;
//    }
//
//    /**
//     * @param topicEvolutionService the topicEvolutionService to set
//     */
//    public void setTopicEvolutionService(TopicEvolutionService topicEvolutionService) {
//        this.topicEvolutionService = topicEvolutionService;
//    }

//    /**
//     * @return the jobName
//     */
//    public String getJobName() {
//        return jobName;
//    }
//
//    /**
//     * @param jobName the jobName to set
//     */
//    public void setJobName(String jobName) {
//        this.jobName = jobName;
//    }
    
}

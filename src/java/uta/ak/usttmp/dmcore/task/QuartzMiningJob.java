/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uta.ak.usttmp.dmcore.task;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;  
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;  
import java.util.List;
import javax.sql.DataSource;
  
import org.apache.log4j.Logger;  
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;  
import org.quartz.JobExecutionContext;  
import org.quartz.PersistJobDataAfterExecution;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import uta.ak.usttmp.common.model.MiningTask;
import uta.ak.usttmp.common.dao.mapper.MiningTaskRowMapper;
import uta.ak.usttmp.common.dao.mapper.RawTextRowMapper;
import uta.ak.usttmp.common.exception.UsttmpProcessException;
import uta.ak.usttmp.common.model.EvolutionRelationship;
import uta.ak.usttmp.common.model.RawText;
import uta.ak.usttmp.common.model.Text;
import uta.ak.usttmp.common.model.Topic;
import uta.ak.usttmp.common.processInterface.MiningComponent;
import uta.ak.usttmp.common.processInterface.PreprocessComponent;
import uta.ak.usttmp.common.processInterface.TrackingComponent;
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
    
    
//    private TopicMiningService topicMiningService;
//    private TopicEvolutionService topicEvolutionService;
    
    private PreprocessComponent preprocessComponent;
    private MiningComponent miningComponent;
    private TrackingComponent trackingComponent;
    
    private MiningTaskService miningTaskService;
    private TopicMiningService topicMiningService;

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
        int totalExecCount=0;
        
        boolean hasPC=false;
        boolean hasTC=false;
        
        MiningTask mt;
        
        try{
            
            miningTaskId=Long.parseLong((String) 
                                         jec.getMergedJobDataMap()
                                             .get("miningTaskId"));
            jobName=(String) jec.getMergedJobDataMap().get("jobName");
            
            applicationContext= new ClassPathXmlApplicationContext("applicationContext.xml");
            
            ds = 
                (DataSource) 
                applicationContext.getBean("dataSource");
            jt=new JdbcTemplate(ds);
            
            //Load Miningtask
            String querySql="select * from c_miningtask where mme_eid=?";
            mt=(MiningTask) jt.queryForObject(querySql, 
                              new Object[]{miningTaskId}, 
                              new MiningTaskRowMapper());
            
            if(mt.getQrtzJobExecCount()==mt.getQrtzJobTotalCount()){
                return;
            }
            totalExecCount=mt.getQrtzJobTotalCount();
            
            if(null!=mt.getPreprocessComponent() && !"NONE".equals(mt.getPreprocessComponent().toUpperCase())){
                hasPC=true;
            }
            if(null!=mt.getTrackingComponent() && !"NONE".equals(mt.getTrackingComponent().toUpperCase())){
                hasTC=true;
            }
            
            
            List<Text> textList;
            List<Topic> topicList;

    
            if(hasPC){
                preprocessComponent=(PreprocessComponent) 
                                applicationContext
                                    .getBean(mt.getPreprocessComponent());
            }
            
            miningComponent=(MiningComponent) 
                                applicationContext
                                    .getBean(mt.getMiningComponent());
            
            if(hasTC){
                trackingComponent=(TrackingComponent) 
                                applicationContext
                                    .getBean(mt.getTrackingComponent());
            }
            
            miningTaskService=(MiningTaskService)
                                  applicationContext
                                        .getBean("miningTaskService");
            
            topicMiningService=(TopicMiningService) 
                                applicationContext
                                    .getBean("topicMiningService");
            
            
            
            
            nextExecCount = mt.getQrtzJobExecCount()+1;
            
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            
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
            
            
            /**************************************************************
             * 1. Preprocess text data                                    *
             **************************************************************/
            String sTag=mt.getId()+"_"+mt.getTag();
                
            //Clear the text table if there is existed text.
            String clearTextSQL="DELETE " +
                                "FROM " +
                                "	c_text " +
                                "WHERE " +
                                "	( " +
                                "       text_createdate " + 
                                "		BETWEEN ? " +
                                "		AND ? " +
                                "	) " +
                                "AND tag = ?";
            jt.update(clearTextSQL, 
                      formatter.format(startTime),
                      formatter.format(endTime),
                      sTag);

            System.out.println("Query raw text records...");
            String querySQL="SELECT " +
                                "	* " +
                                "FROM " +
                                "	c_rawtext " +
                                "WHERE " +
                                "	( " +
                                "       text_createdate " + 
                                "		BETWEEN ? " +
                                "		AND ? " +
                                "	) " +
                                "AND tag = ?";

            List<RawText> rawTextList=jt.query(querySQL, 
                                                new Object[]{formatter.format(startTime),
                                                             formatter.format(endTime),
                                                             mt.getTag()}, 
                                                new RawTextRowMapper());
            
            if(hasPC){
                
                textList=preprocessComponent.preprocess(mt, rawTextList);
            }else{
                
                textList=new ArrayList<>();
                for(RawText rt: rawTextList){
                    Text tx=new Text();
                    tx.setCreateTime(rt.getCreateTime());
                    tx.setRawTextId(rt.getId());
                    tx.setTag(sTag);
                    tx.setText(rt.getText());
                    tx.setTitle(rt.getTitle());
                    
                    textList.add(tx);
                }
            }
            
            List<Object[]> text_lines=new ArrayList<>();
            for(Text tx : textList){
                Object[] ojarr=new Object[]{ tx.getTitle(),
                                              tx.getText(),
                                              sTag,
                                              String.valueOf(tx.getRawTextId()),
                                              formatter.format(tx.getCreateTime()) };
                text_lines.add(ojarr);
            }

            String insertSQL="INSERT INTO c_text(mme_lastupdate, mme_updater, title, text, tag, rawtext_id, text_createdate) "
                                + "VALUES (NOW(), \"USTTMP\", ?, ?, ?, ?, ?)";

            System.out.println("Start to insert text records...");
            jt.batchUpdate(insertSQL, text_lines);
            
            
            /**************************************************************
             * 2. Mining topics                                           *
             **************************************************************/
            
            //Clear the existed topics
            
            String clearTopicSQL = "DELETE " +
                                "FROM " +
                                "	c_topic " +
                                "WHERE " +
                                "miningtask_id=? AND seq_no = ?";
            jt.update(clearTopicSQL, miningTaskId, nextExecCount);
            
            topicList=miningComponent.generateTopics(mt, textList);
            
            String insertTpSQL="INSERT INTO `c_topic` ( " +
                            "	`mme_lastupdate`, " +
                            "	`mme_updater`, " +
                            "	`name`, " +
                            "	`content`, " +
                            "	`remark`, " + 
                            "	`miningtask_id`, " +
                            "	`seq_no` " +
                            ") " +
                            "VALUES " +
                            "	(NOW(), 'USTTMP' ,?,?,?,?,?)";

            List<Object[]> tpArgsList=new ArrayList<>();

            for (Topic tm : topicList){
                Object[] objarr=new Object[]{tm.getName(),
                                             tm.toString(),
                                             (null!=tm.getRemark()) ? tm.getRemark() : "",
                                             miningTaskId,
                                             nextExecCount};
                tpArgsList.add(objarr);
            }

            System.out.println("Inserting records into the c_topic table...");
            jt.batchUpdate(insertTpSQL, tpArgsList);
        
            
            
            /**************************************************************
             * 3. Evolution tracking                                      *
             **************************************************************/
            
            if(hasTC){
                if(nextExecCount>1){
                    if(null!=topicList && !topicList.isEmpty()){
                        
                        int preTopicSeq=nextExecCount-1;
                        int nextTopicSeq=nextExecCount;
                        
                        //Clear existed topic evolution rela
                        String clearEvSQL="DELETE " +
                                        "FROM " +
                                        "	c_topicevolutionrela " +
                                        "WHERE " +
                                        "	miningtask_id =? " +
                                        "AND pre_topic_seq =? " +
                                        "AND next_topic_seq =?";
                        jt.update(clearEvSQL,miningTaskId,
                                                      preTopicSeq,
                                                      nextTopicSeq);
        
                        List<Topic> preTopics=topicMiningService.getTopics(miningTaskId, preTopicSeq);
                        List<Topic> nextTopics=topicMiningService.getTopics(miningTaskId, nextTopicSeq);

                        if(null==preTopics || preTopics.isEmpty()){

                            UsttmpProcessException upe
                                = new UsttmpProcessException(UsttmpProcessException.TYPE_CALC_EVO_RELA_EXCEPTION);
                            throw upe;
                        }
                        if(null==nextTopics || nextTopics.isEmpty()){
                            UsttmpProcessException upe
                                = new UsttmpProcessException(UsttmpProcessException.TYPE_CALC_EVO_RELA_EXCEPTION);
                            throw upe;
                        }
        
                        List<EvolutionRelationship> evRelaList = trackingComponent
                                                                    .getTopicEvolutionRelationships(mt, preTopics, nextTopics);
        
                        String insertEvSql="INSERT INTO `c_topicevolutionrela` (  " +
                                            "	`pre_topic_id`,  " +
                                            "	`next_topic_id`,  " +
                                            "	`rank_against_pre_topic_in_next_group`,  " +
                                            "	`rank_against_next_topic_in_pre_group`,  " +
                                            "	`similarity`  ," +
                                            "	`miningtask_id`  ," +
                                            "	`pre_topic_seq`  ," +
                                            "	`next_topic_seq`  " +
                                            ")  " +
                                            "VALUES  " +
                                            "	(?, ?, ?, ?, ?, ?, ?, ?)";
        
                        List<Object[]> argsList=new ArrayList<>();

                        for (EvolutionRelationship er : evRelaList){
                            Object[] objarr=new Object[]{er.getPreTopic().getId(),
                                                         er.getNextTopic().getId(),
                                                         er.getRankAgainstPreTopicInNextGroup(),
                                                         er.getRankAgainstNextTopicInPreGroup(),
                                                         er.getSimilarity(),
                                                         miningTaskId,
                                                         preTopicSeq,
                                                         nextTopicSeq};
                            argsList.add(objarr);
                        }
                        jt.batchUpdate(insertEvSql, argsList);
                    }
                }
            }
            
            int nowStatus=(nextExecCount==totalExecCount) ? MiningTask.STATUS_COMPLETED : MiningTask.STATUS_RUNNING;
            //Update task status
            jt.update(updateSql, 
                      jobName,
                      nextExecCount,
                      nowStatus,
                      miningTaskId);
            
        }catch(UsttmpProcessException e){
            
            if(UsttmpProcessException
                   .TYPE_CALC_EVO_RELA_EXCEPTION.equals(e.getMessage())){
                
                //Update task status
                int nowStatus=(nextExecCount==totalExecCount) ? MiningTask.STATUS_COMPLETED : MiningTask.STATUS_RUNNING;
                jt.update(updateSql, 
                          jobName,
                          nextExecCount,
                          nowStatus,
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
    
}

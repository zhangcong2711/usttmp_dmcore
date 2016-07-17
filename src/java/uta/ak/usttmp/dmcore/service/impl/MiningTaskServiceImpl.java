/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uta.ak.usttmp.dmcore.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import uta.ak.usttmp.common.dao.UsttmpDaoSupport;
import uta.ak.usttmp.common.dao.mapper.MiningTaskRowMapper;
import uta.ak.usttmp.common.model.MiningTask;
import uta.ak.usttmp.dmcore.service.MiningTaskService;
import uta.ak.usttmp.dmcore.task.QuartzMiningJob;

/**
 *
 * @author zhangcong
 */
public class MiningTaskServiceImpl extends UsttmpDaoSupport 
                                   implements MiningTaskService {
    
    
    @Autowired
    private Scheduler quartzScheduler;
    
    @Override
    public void logMiningTask(int type, 
                              long miningTaskId, 
                              String content) {
        
        String sql="INSERT INTO `c_miningtask_log` ( " +
                    "	`type`, " +
                    "	`mingingtask_id`, " +
                    "	`exception_info`, " +
                    "	`exception_time` " +
                    ") " +
                    "VALUES " +
                    "	(?, ?, ?, now());";
        
        this.getJdbcTemplate().update(sql, type, miningTaskId, content);
    }

    @Override
    public int addMiningTask(MiningTask mt) throws Exception {
        
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String startdt=formatter.format(mt.getStartTime());
        String enddt=formatter.format(mt.getEndTime());
        
        int count=(int) ((mt.getEndTime().getTime()-mt.getStartTime().getTime()) /
                         (mt.getMiningInterval()*60*60*1000));
        mt.setQrtzJobTotalCount(count);
        
        long taskId=this.doCreateSingleMiningTask(mt);
        
        Calendar cal = Calendar.getInstance();
        cal.setTime(mt.getStartTime());
        cal.add(Calendar.HOUR_OF_DAY,mt.getMiningInterval());
        Date jobStartTime=cal.getTime();
        
        this.scheduleJob(taskId, 
                         mt.getName(), 
                         count,
                         mt.getMiningInterval(),
                         jobStartTime);
        
        return 1;
    }
    
    protected long doCreateSingleMiningTask(MiningTask mt){
        
        if(checkDuplTask(mt.getName())){
            throw new IllegalArgumentException("The name of mining task is duplicated.");
        }
        
        
        final String insertSql="INSERT INTO `c_miningtask` ( " +
                                            "	`name`, " +
                                            "	`starttime`, " +
                                            "	`endtime`, " +
                                            "	`mininginterval`, " +
                                            "	`status`, " +
                                            "	`tag`, " +
                                            "	`qrtz_job_exec_count`, " +
                                            "	`qrtz_job_total_count`," +
                                            "	`topic_num`," +
                                            "	`keyword_num`," +
                                            "	`alpha`," +
                                            "	`beta`" +
//                                            "	`qrtz_job_name` " +
                                            ") " +
                                            "VALUES " +
                                            "	( " +
                                            "		?, " +
                                            "		?, " +
                                            "		?, " +
                                            "		?, " +
                                            "		?, " +
                                            "		?, " +
                                            "		?, " +
                                            "		?, " +
                                            "		?, " +
                                            "		?, " +
                                            "		?, " +
                                            "		? )";
        
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
    	KeyHolder keyHolder = new GeneratedKeyHolder();
    	JdbcTemplate jt=this.getJdbcTemplate();
        
        jt.update(
            new PreparedStatementCreator() {
                public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
                    PreparedStatement pst =
                        con.prepareStatement(insertSql, new String[] {"mme_eid"});
                    pst.setString(1, mt.getName());
                    pst.setString(2, formatter.format(mt.getStartTime()));
                    pst.setString(3, formatter.format(mt.getEndTime()));
                    pst.setInt(4, mt.getMiningInterval());
                    pst.setInt(5, MiningTask.STATUS_NOT_STARTED);
                    pst.setString(6, mt.getTag());
                    pst.setInt(7, 0);
                    pst.setInt(8, mt.getQrtzJobTotalCount());
                    pst.setInt(9, mt.getTopicNum());
                    pst.setInt(10, mt.getKeywordNum());
                    pst.setDouble(11, mt.getAlpha());
                    pst.setDouble(12, mt.getBeta());
//                    pst.setString(9, );
                    return pst;
                }
            },
            keyHolder);
        
        long taskId= (long) keyHolder.getKey();
        
        return taskId;
    }
    
    protected void scheduleJob(long taskId,
                               String name, 
                               int count, 
                               int miningInterval,
                               Date triggerStartTime) throws Exception {
        
        
        JobDetail jobDetail = JobBuilder.newJob(QuartzMiningJob.class)
                .withIdentity("qrtz_job_"+name, "qrtz_job_"+name)  
                .usingJobData("miningTaskId", String.valueOf(taskId))
                .usingJobData("jobName", "qrtz_job_"+name)
                .build();  
        SimpleScheduleBuilder builder = SimpleScheduleBuilder
                .simpleSchedule()
                .repeatSecondlyForTotalCount(count).withIntervalInHours(miningInterval);  
        Trigger trigger = TriggerBuilder.newTrigger()  
                .withIdentity("qrtz_trigger_"+name, 
                              "qrtz_trigger_"+name).startAt(triggerStartTime)
                .withSchedule(builder).build();  
        
        quartzScheduler.scheduleJob(jobDetail, trigger);  
    }
    
    private boolean checkDuplTask(String name) {
        
        String querySql="select * from c_miningtask where name=?";
        List lt=this.getJdbcTemplate().query(querySql, 
                                             new MiningTaskRowMapper(), 
                                             name);
        return (null!=lt && !lt.isEmpty());
    }

    @Override
    public int updateMiningTask(MiningTask mt) throws Exception  {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int deleteMiningTask(long miningTaskId) throws Exception  {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int stopMiningTask(long miningTaskId) throws Exception  {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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

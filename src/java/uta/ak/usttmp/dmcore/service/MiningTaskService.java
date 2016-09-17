/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uta.ak.usttmp.dmcore.service;

import uta.ak.usttmp.common.model.MiningTask;

/**
 *
 * @author zhangcong
 */
public interface MiningTaskService {
    
    public static final int LOG_TYPE_EXCEPTION=2;
    public static final int LOG_TYPE_INFO=1;
    
    public int addMiningTask(MiningTask mt) throws Exception ;
    
    public int updateMiningTask(MiningTask mt) throws Exception ;
    
    public int deleteMiningTask(long miningTaskId) throws Exception ;
    
    public int stopMiningTask(long miningTaskId) throws Exception ;
    
    public void updateMiningTaskStatus(long miningTaskId, int status) throws Exception;
    
    public void logMiningTask(int type, long miningTaskId, String content);
}

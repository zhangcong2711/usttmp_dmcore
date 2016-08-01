/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uta.ak.usttmp.dmcore.systeminterface;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.lang3.StringEscapeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import uta.ak.usttmp.common.model.MiningTask;
import uta.ak.usttmp.common.systemconst.UsttmpConst;
import uta.ak.usttmp.common.systeminterface.UsttmpInterfaceManager;
import uta.ak.usttmp.common.systeminterface.model.CallResult;
import uta.ak.usttmp.common.systeminterface.model.Message;
import uta.ak.usttmp.common.util.UsttmpXmlUtil;
import uta.ak.usttmp.dmcore.service.MiningTaskService;

/**
 *
 * @author zhangcong
 */
public class DmcoreInterfaceImpl implements UsttmpInterfaceManager{
    
    @Autowired
    private MiningTaskService miningTaskService;

    @Override
    public Message call(String targetName, 
                        String invokeType, 
                        String methodName, 
                        String methodBody) throws Exception{
        
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Message response(String msgstr) throws Exception{
        
        Message callMsg=(Message) 
                         UsttmpXmlUtil.xmlStrToObject( 
                             StringEscapeUtils.unescapeXml(msgstr), 
                             Message.class);
        
        String msgMethod=callMsg.getMethodName();
        String msgBody=callMsg.getMethodBody();
        
        Message responseMsg=new Message();
        responseMsg.setSource(UsttmpConst.SUBSYSTEM_NAME_DMCORE);
        responseMsg.setTarget(UsttmpConst.SUBSYSTEM_NAME_CONSOLE);
        
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dt=formatter.format(new Date());
        responseMsg.setTimeStamp(dt);
        
        responseMsg.setType(UsttmpInterfaceManager.TYPE_RESPONSE);
        
        CallResult cr=new CallResult();
        
        try{
            switch(msgMethod){
                
                case "AddMiningTask":
                    
                    miningTaskService.addMiningTask((MiningTask) 
                                                     UsttmpXmlUtil.
                                                         xmlStrToObject(msgBody, 
                                                                        MiningTask.class));
                    break;

                case "StopMiningTask":
                    break;

                case "DeleteMiningTask":
                    break;
                    
                case "CheckStatus":
                    break;

                default:
                    throw new IllegalArgumentException("No such method");
            }
            
            cr.setInfo("Operation done successfully.");
            cr.setResultStatus(CallResult.RESULT_SUCCESS);
            
        }catch(Exception e){
            e.printStackTrace();
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            cr.setError(errors.toString());
            cr.setResultStatus(CallResult.RESULT_FAILED);
        }finally{
            
            responseMsg.setMethodBody(UsttmpXmlUtil.objectToXmlStr(cr, cr.getClass(), true));
            return responseMsg;
        }
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
    
}

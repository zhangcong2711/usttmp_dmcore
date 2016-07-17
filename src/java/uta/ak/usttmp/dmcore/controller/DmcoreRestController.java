package uta.ak.usttmp.dmcore.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;
import uta.ak.usttmp.common.systeminterface.UsttmpInterfaceManager;
import uta.ak.usttmp.common.systeminterface.model.Message;
import uta.ak.usttmp.common.util.UsttmpXmlUtil;
import uta.ak.usttmp.common.web.controller.UsttmpRestController;

@RestController
@RequestMapping("rest")
public class DmcoreRestController extends UsttmpRestController {
    
    @Autowired
    private UsttmpInterfaceManager dmcoreInterface;

    @Override
    @RequestMapping(value = "/interfaceResponser", 
                    method = RequestMethod.POST, 
                    produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> getMessage(@RequestBody String message) throws Exception {
        
        Message responseMsg=dmcoreInterface.response(message);
        return new ResponseEntity<String>(UsttmpXmlUtil.objectToXmlStr(responseMsg, 
                                                                       Message.class,
                                                                       false), 
                                          HttpStatus.OK);
    }

    /**
     * @return the dmcoreInterface
     */
    public UsttmpInterfaceManager getDmcoreInterface() {
        return dmcoreInterface;
    }

    /**
     * @param dmcoreInterface the dmcoreInterface to set
     */
    public void setDmcoreInterface(UsttmpInterfaceManager dmcoreInterface) {
        this.dmcoreInterface = dmcoreInterface;
    }
    
    
    
}

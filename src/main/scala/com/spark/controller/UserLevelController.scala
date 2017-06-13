package com.spark.controller

import com.spark.service.UserLevelService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Controller
import org.springframework.ui.Model
import org.springframework.web.bind.annotation._

/**
 * Created by Victor  on 17-6-2.
 */
@Controller
@RequestMapping(Array("/spark/user/level"))
class UserLevelController @Autowired()(val userLevelService: UserLevelService) {

    @RequestMapping(value = Array("/test"), method = Array(RequestMethod.GET))
    @ResponseBody
    def listHttpSuiteTestCaseJson(model: Model) = {
        userLevelService.schedulCluster
    }
}

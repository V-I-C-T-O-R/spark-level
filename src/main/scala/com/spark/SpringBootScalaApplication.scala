package com.spark

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.web.support.SpringBootServletInitializer

/**
 * Created by Victor on 17-6-2.
 * 部署在外部容器中
 */
@SpringBootApplication
class SpringBootScalaApplication extends SpringBootServletInitializer{
    override def configure(application: SpringApplicationBuilder): SpringApplicationBuilder = application.sources(classOf[SpringBootScalaApplication])
}

/*object SpringBootScalaApplication extends App {
    SpringApplication.run(classOf[SpringBootScalaApplication])
}*/

object SpringBootScalaApplication {
    def main(args : Array[String]): Unit ={
        SpringApplication.run(classOf[SpringBootScalaApplication])
    }
}

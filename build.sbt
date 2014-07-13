name := "DW Hive Server"

version := "0.0.1"

organization := "com.anjuke.dw"

scalaVersion := "2.10.0"

resolvers ++= Seq("snapshots"     at "http://oss.sonatype.org/content/repositories/snapshots",
                  "staging"       at "http://oss.sonatype.org/content/repositories/staging",
                  "releases"      at "http://oss.sonatype.org/content/repositories/releases",
                  "cloudera"      at "https://repository.cloudera.com/artifactory/cloudera-repos/",
                  "spring"        at "http://repo.springsource.org/libs-release-remote/"
                 )

seq(webSettings :_*)

unmanagedResourceDirectories in Test <+= (baseDirectory) { _ / "src/main/webapp" }

scalacOptions ++= Seq("-deprecation", "-unchecked")

libraryDependencies ++= {
  val liftVersion = "2.5.1"
  Seq(
    "net.liftweb"       %% "lift-webkit"        % liftVersion        % "compile",
    "net.liftweb"       %% "lift-mapper"        % liftVersion        % "compile",
    "net.liftmodules"   %% "lift-jquery-module_2.5" % "2.4",
    "org.eclipse.jetty" % "jetty-webapp"        % "8.1.7.v20120910"  % "container,test",
    "org.eclipse.jetty" % "jetty-plus"          % "8.1.7.v20120910"  % "container,test",
    "org.eclipse.jetty.orbit" % "javax.servlet" % "3.0.0.v201112011016" % "container,test" artifacts Artifact("javax.servlet", "jar", "jar"),
    "ch.qos.logback"    % "logback-classic"     % "1.0.6",
    "org.specs2"        %% "specs2"             % "1.14"             % "test",
    "mysql" % "mysql-connector-java" % "5.1.26",
    "com.typesafe.akka" %% "akka-actor" % "2.1.2",
    "com.typesafe.akka" %% "akka-remote" % "2.1.2",
    "net.databinder.dispatch" %% "dispatch-core" % "0.11.1",
    "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.5.0",
    "org.apache.hive" % "hive-jdbc" % "0.10.0-cdh4.5.0"
  )
}

env in Compile := Some(file(".") / "jetty-env.xml" asFile)

port in container.Configuration := 8082

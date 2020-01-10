import Settings._

val `distributedarchitectures` = project
  .in(file("."))
  .enablePlugins(DeployApp, DockerPlugin)
  .settings(defaultSettings: _*)
  .settings(
     libraryDependencies ++= Dependencies.Dist, parallelExecution in Test := false,
       libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
)



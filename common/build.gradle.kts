description = "Typical implementation of similarity measures, duplicate detection strategies, cluster algorithms, and fusion."

dependencies {
    "api"(project(":core"))
    "api"(group = "org.apache.commons", name = "commons-text", version = "1.11.0")
    "api"(group = "commons-codec", name = "commons-codec", version = "1.16.0")

    implementation(group = "com.google.guava", name = "guava", version = "33.0.0-jre")
    implementation(group = "org.jgrapht", name = "jgrapht-core", version = "1.5.2")
}

dependencies {
    "api"(project(":core"))

    "api"(group = "org.apache.commons", name = "commons-text", version = "1.4")
    "api"(group = "commons-codec", name = "commons-codec", version = "1.11")
    implementation(group = "com.google.guava", name = "guava", version = "26.0-jre")
}
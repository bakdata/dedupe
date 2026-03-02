description = "Typical implementation of similarity measures, duplicate detection strategies, cluster algorithms, and fusion."

dependencies {
    api(project(":core"))
    api(libs.commons.text)
    api(libs.commons.codec)

    implementation(libs.guava)
    implementation(libs.jgrapht)
}

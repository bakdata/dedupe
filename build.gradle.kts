plugins {
    // release
    jacoco
    id("net.researchgate.release") version "2.6.0"
    id("com.bakdata.sonar") version "1.1.4"
    id("com.bakdata.sonatype") version "1.1.4"
    id("org.hildan.github.changelog") version "0.8.0"
    id("io.freefair.lombok") version "3.1.4" apply false
}

allprojects {
    group = "com.bakdata.dedupe"

    tasks.withType<Test> {
        maxParallelForks = 4
    }

    repositories {
        mavenCentral()
    }
}

configure<com.bakdata.gradle.SonatypeSettings> {
    developers {
        developer {
            name.set("Arvid Heise")
            id.set("AHeise")
        }
        developer {
            name.set("Philipp Schirmer")
        }
    }
}

configure<org.hildan.github.changelog.plugin.GitHubChangelogExtension> {
    githubUser = "bakdata"
    futureVersionTag = findProperty("changelog.releaseVersion")?.toString()
    sinceTag = findProperty("changelog.sinceTag")?.toString()
}

tasks.register<Javadoc>("javadoc") {
    description = "Generates a global javadoc from all the modules"
    options {
        (this as StandardJavadocDocletOptions).apply {
            addBooleanOption("html5", true)
            stylesheetFile(File("${rootDir}/src/main/javadoc/assertj-javadoc.css"))
            addBooleanOption("-allow-script-in-comments", true)
            header("<script src=\"http://cdn.jsdelivr.net/highlight.js/8.6/highlight.min.js\"></script>")
            footer("<script type=\"text/javascript\">\nhljs.initHighlightingOnLoad();\n</script>")
            tags("apiNote:a:API Note:", "implSpec:a:Implementation Requirements:", "implNote:a:Implementation Note:", "sneaky:a:Sneaky Throws:")
        }
    }
    val exportedProjects = listOf(project(":core"), project(":common"))
    setSource(exportedProjects.map { it.tasks.named("delombok") })
    classpath = files(exportedProjects.map { it.the<SourceSetContainer>()["main"].compileClasspath })
    setDestinationDir(file("$buildDir/docs/javadoc"))
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "io.freefair.lombok")

    configure<JavaPluginConvention> {
        sourceCompatibility = org.gradle.api.JavaVersion.VERSION_11
        targetCompatibility = org.gradle.api.JavaVersion.VERSION_11
    }

    tasks.withType<Javadoc> {
        options {
            (this as StandardJavadocDocletOptions).apply {
                addBooleanOption("html5", true)
                stylesheetFile(File("${rootDir}/src/main/javadoc/assertj-javadoc.css"))
                addBooleanOption("-allow-script-in-comments", true)
                header("<script src=\"http://cdn.jsdelivr.net/highlight.js/8.6/highlight.min.js\"></script>")
                footer("<script type=\"text/javascript\">\nhljs.initHighlightingOnLoad();\n</script>")
                tags("apiNote:a:API Note:", "implSpec:a:Implementation Requirements:", "implNote:a:Implementation Note:", "sneaky:a:Sneaky Throws:")
            }
        }
        setSource(tasks.named("delombok"))
    }

    dependencies {
        "testImplementation"("org.junit.jupiter:junit-jupiter-api:5.3.0")
        "testImplementation"("org.junit.jupiter:junit-jupiter-params:5.3.0")
        "testRuntimeOnly"("org.junit.jupiter:junit-jupiter-engine:5.3.0")
        "testImplementation"(group = "org.assertj", name = "assertj-core", version = "3.13.0")

        "compileOnly"("org.projectlombok:lombok:1.18.6")
        "annotationProcessor"("org.projectlombok:lombok:1.18.6")
        "testCompileOnly"("org.projectlombok:lombok:1.18.6")
        "testAnnotationProcessor"("org.projectlombok:lombok:1.18.6")
    }
}

val jacocoMerge by tasks.registering(JacocoMerge::class) {
    subprojects {
        dependsOn(tasks.withType<JacocoReport>())
        executionData(tasks.withType<JacocoReport>().map { it.executionData })
    }
    destinationFile = file("$buildDir/jacoco")
}
tasks.register<JacocoReport>("jacocoRootReport") {
    dependsOn(jacocoMerge)
    sourceDirectories.from(files(subprojects.map { it.the<SourceSetContainer>()["main"].allSource.srcDirs }))
    classDirectories.from(files(subprojects.map { it.the<SourceSetContainer>()["main"].output }))
    executionData(jacocoMerge.get().destinationFile)
    reports {
        html.isEnabled = true
        xml.isEnabled = true
        csv.isEnabled = false
    }
}
allprojects {
    // using a newer feature of sonarqube to use the xml reports which also makes it language-agnostic
    configure<org.sonarqube.gradle.SonarQubeExtension> {
        properties {
            property("sonar.coverage.jacoco.xmlReportPaths",
                    rootProject.tasks.withType<JacocoReport>().map { it.reports.xml.destination })
        }
    }
}

rootProject.tasks.named("sonarqube") { dependsOn(tasks.withType<JacocoReport>()) }

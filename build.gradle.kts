plugins {
    // release
    id("net.researchgate.release") version "2.8.1"
    id("com.bakdata.sonar") version "1.1.7"
    id("com.bakdata.sonatype") version "1.1.7"
    id("org.hildan.github.changelog") version "1.7.0"
    id("io.freefair.lombok") version "5.3.3.3" apply false
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

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "io.freefair.lombok")

    configure<JavaPluginConvention> {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    dependencies {
        "testImplementation"("org.junit.jupiter:junit-jupiter-api:5.7.2")
        "testImplementation"("org.junit.jupiter:junit-jupiter-params:5.7.2")
        "testRuntimeOnly"("org.junit.jupiter:junit-jupiter-engine:5.7.2")
        "testImplementation"(group = "org.assertj", name = "assertj-core", version = "3.13.0")
    }
}

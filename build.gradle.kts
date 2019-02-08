plugins {
    `java-library`
    // release
    id("net.researchgate.release") version "2.6.0"
    id("com.bakdata.sonar") version "1.0.1"
    id("com.bakdata.sonatype") version "1.0.1"
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

subprojects {
    apply(plugin = "java-library")
    // build fails for java 11, let"s wait for a newer lombok version
    configure<JavaPluginConvention> {
        sourceCompatibility = org.gradle.api.JavaVersion.VERSION_11
        targetCompatibility = org.gradle.api.JavaVersion.VERSION_11
    }

    dependencies {
        implementation(group= "com.google.guava", name= "guava", version= "26.0-jre")

        testImplementation("org.junit.jupiter:junit-jupiter-api:5.3.0")
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.3.0")
        testImplementation(group= "org.assertj", name= "assertj-core", version= "3.11.1")

        compileOnly("org.projectlombok:lombok:1.18.4")
        annotationProcessor("org.projectlombok:lombok:1.18.4")
        testCompileOnly("org.projectlombok:lombok:1.18.4")
        testAnnotationProcessor("org.projectlombok:lombok:1.18.4")
    }
}
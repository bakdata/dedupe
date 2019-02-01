plugins {
    // kotlin stuff
    `kotlin-dsl`
    // release
    id("net.researchgate.release") version "2.6.0"
    // eat your own dog food - apply the plugins to this plugin project
    id("com.bakdata.sonar") version "1.0.0"
    id("com.bakdata.sonatype") version "1.0.0"
    id("io.franzbecker.gradle-lombok") version "1.14"
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
    apply(plugin = "io.franzbecker.gradle-lombok")
    lombok {
        version = "1.18.4"
        sha256 = ""
    }

    // build fails for java 11, let"s wait for a newer lombok version
    configure<JavaPluginConvention> {
        sourceCompatibility = org.gradle.api.JavaVersion.VERSION_1_10
        targetCompatibility = org.gradle.api.JavaVersion.VERSION_1_10
    }

    dependencies {
        implementation(group= "com.google.guava", name= "guava", version= "26.0-jre")

        testImplementation("org.junit.jupiter:junit-jupiter-api:5.3.0")
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.3.0")
        testImplementation(group= "org.assertj", name= "assertj-core", version= "3.11.1")
    }
}

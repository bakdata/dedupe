buildscript {
    repositories {
        maven {
            url = uri("https://s01.oss.sonatype.org/content/repositories/snapshots")
        }
    }
    dependencies {
        classpath("com.bakdata.gradle:sonar:1.5.3-SNAPSHOT")
        classpath("com.bakdata.gradle:release:1.5.3-SNAPSHOT")
        classpath("com.bakdata.gradle:sonatype:1.5.3-SNAPSHOT")
    }
}

plugins {
    id("io.freefair.lombok") version "8.4" apply false
}
apply(plugin = "com.bakdata.release")
apply(plugin = "com.bakdata.sonar")
apply(plugin = "com.bakdata.sonatype")

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

tasks.register<Javadoc>("javadoc") {
    description = "Generates a global javadoc from all the modules"
    options {
        (this as StandardJavadocDocletOptions).apply {
            addBooleanOption("html5", true)
            stylesheetFile(File("${rootDir}/src/main/javadoc/assertj-javadoc.css"))
            addBooleanOption("-allow-script-in-comments", true)
            header("<script src=\"http://cdn.jsdelivr.net/highlight.js/8.6/highlight.min.js\"></script>")
            footer("<script type=\"text/javascript\">hljs.initHighlightingOnLoad();</script>")
            tags("apiNote:a:API Note:", "implSpec:a:Implementation Requirements:", "implNote:a:Implementation Note:", "sneaky:a:Sneaky Throws:")
        }
    }
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "io.freefair.lombok")

    configure<JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(11)
        }
    }

    tasks.withType<Javadoc> {
        options {
            (this as StandardJavadocDocletOptions).apply {
                addBooleanOption("html5", true)
                stylesheetFile(File("${rootDir}/src/main/javadoc/assertj-javadoc.css"))
                addBooleanOption("-allow-script-in-comments", true)
                header("<script src=\"http://cdn.jsdelivr.net/highlight.js/8.6/highlight.min.js\"></script>")
                footer("<script type=\"text/javascript\">hljs.initHighlightingOnLoad();</script>")
                tags("apiNote:a:API Note:", "implSpec:a:Implementation Requirements:", "implNote:a:Implementation Note:", "sneaky:a:Sneaky Throws:")
            }
        }
    }

    dependencies {
        val junitVersion = "5.11.4"
        "testImplementation"("org.junit.jupiter:junit-jupiter-api:$junitVersion")
        "testImplementation"("org.junit.jupiter:junit-jupiter-params:$junitVersion")
        "testRuntimeOnly"("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
        "testImplementation"(group = "org.assertj", name = "assertj-core", version = "3.27.2")
    }
}

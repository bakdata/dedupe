plugins {
    // release
    id("net.researchgate.release") version "3.0.2"
    id("com.bakdata.sonar") version "1.1.11"
    id("com.bakdata.sonatype") version "1.1.11"
    id("org.hildan.github.changelog") version "1.12.1"
    id("io.freefair.lombok") version "6.6.3" apply false
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
            footer("<script type=\"text/javascript\">hljs.initHighlightingOnLoad();</script>")
            tags("apiNote:a:API Note:", "implSpec:a:Implementation Requirements:", "implNote:a:Implementation Note:", "sneaky:a:Sneaky Throws:")
        }
    }
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "io.freefair.lombok")

    configure<JavaPluginExtension> {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
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
        "testImplementation"("org.junit.jupiter:junit-jupiter-api:5.10.1")
        "testImplementation"("org.junit.jupiter:junit-jupiter-params:5.10.1")
        "testRuntimeOnly"("org.junit.jupiter:junit-jupiter-engine:5.10.1")
        "testImplementation"(group = "org.assertj", name = "assertj-core", version = "3.25.1")
    }
}

release {
    git {
        requireBranch.set("master")
    }
}

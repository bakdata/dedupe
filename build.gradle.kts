plugins {
    alias(libs.plugins.release)
    alias(libs.plugins.sonar)
    alias(libs.plugins.sonatype)
    alias(libs.plugins.lombok) apply false
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

tasks.register<Javadoc>("javadoc") {
    description = "Generates a global javadoc from all the modules"
    options {
        (this as StandardJavadocDocletOptions).apply {
            addBooleanOption("html5", true)
            stylesheetFile(File("${rootDir}/src/main/javadoc/assertj-javadoc.css"))
            addBooleanOption("-allow-script-in-comments", true)
            header("<script src=\"http://cdn.jsdelivr.net/highlight.js/8.6/highlight.min.js\"></script>")
            footer("<script type=\"text/javascript\">hljs.initHighlightingOnLoad();</script>")
            tags(
                "apiNote:a:API Note:",
                "implSpec:a:Implementation Requirements:",
                "implNote:a:Implementation Note:",
                "sneaky:a:Sneaky Throws:"
            )
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

    publication {
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

    tasks.withType<Javadoc> {
        options {
            (this as StandardJavadocDocletOptions).apply {
                addBooleanOption("html5", true)
                stylesheetFile(File("${rootDir}/src/main/javadoc/assertj-javadoc.css"))
                addBooleanOption("-allow-script-in-comments", true)
                header("<script src=\"http://cdn.jsdelivr.net/highlight.js/8.6/highlight.min.js\"></script>")
                footer("<script type=\"text/javascript\">hljs.initHighlightingOnLoad();</script>")
                tags(
                    "apiNote:a:API Note:",
                    "implSpec:a:Implementation Requirements:",
                    "implNote:a:Implementation Note:",
                    "sneaky:a:Sneaky Throws:"
                )
            }
        }
    }

    dependencies {
        "testRuntimeOnly"(rootProject.libs.junit.platform.launcher)
        "testImplementation"(rootProject.libs.junit.jupiter)
        "testImplementation"(rootProject.libs.assertj)
    }
}

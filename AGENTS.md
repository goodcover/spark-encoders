# Repository Guidelines

## Project Structure & Module Organization

This is an sbt Scala library for Spark `Encoder` derivation. Shared implementation lives in
`src/main/scala/io/github/pashashiz/spark_encoders`. Version-specific derivation code lives in
`src/main/scala-2` and `src/main/scala-3`; Spark-version shims live in `src/main/scala-spark3`
and `src/main/scala-spark4`. Tests follow the same split under `src/test/scala`,
`src/test/scala-2`, and `src/test/scala-3`. Test resources are in `src/test/resources`, project
versions are centralized in `project/Versions.scala`, and design notes live in `docs/`.

## Build, Test, and Development Commands

- `sbt compile`: compile the default Scala/Spark combination.
- `sbt test`: run tests for the default Scala version.
- `sbt +test`: run the test suite across configured Scala versions.
- `sbtn ++3.3.6 testOnly io.github.pashashiz.spark_encoders.AnyValEncoderSpec`: run one spec on
  Scala 3 using the sbt client.
- `SPARK_VERSION=4.0.1 sbt clean +test`: test against a supported Spark version.
- `SPARK_VERSION=4.0.1 sbt "++ 2.13.16" clean "Test / assembly"`: build the test assembly for
  Databricks or external Spark validation.

## Coding Style & Naming Conventions

Use Scala style already present in the repository: two-space indentation, concise comments, and
100-column wrapping. The project includes `.scalafmt.conf` with Scala 3 dialect,
`defaultWithAlign`, sorted imports/modifiers, and `docstrings.style = SpaceAsterisk`; run scalafmt
before submitting when available. Keep package names under `io.github.pashashiz.spark_encoders`.
Name encoders as `XEncoder`, invariants as `XInvariant`, and specs as `XSpec`.

## Testing Guidelines

Tests use ScalaTest `AnyWordSpec` via `SparkAnyWordSpec` and matcher helpers in
`TypedEncoderMatchers`. Add shared tests to `src/test/scala`; add Scala-version-specific coverage
only when behavior or syntax differs. Prefer focused commands such as `testOnly ...Spec` while
iterating, then run the relevant cross-version command before a PR. Cover both schema expectations
and dataset round trips for encoder changes.

## Commit & Pull Request Guidelines

Recent commits use short imperative subjects, sometimes with an area prefix, for example
`scala3: Make nested struct fields nullable for UpCast` or `Handle generic value class fields in
case classes`. Keep commits scoped and avoid unrelated IDE or generated-file churn. PRs should
explain the behavioral change, list Spark/Scala combinations tested, link related issues when
available, and call out compatibility risks around Catalyst expressions, nullability, or
cross-version shims.

## Configuration Tips

Set `SPARK_VERSION` to one of the supported values in `project/Versions.scala`. Use Java 8 for
Spark 3.x validation and Java 17+ for Spark 4.x validation, matching `CONTRIBUTING.md`.

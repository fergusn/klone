FROM openjdk:11-jdk-slim as build
COPY . /src
WORKDIR /src
RUN ./gradlew installDist

FROM openjdk:11-jre 
COPY --from=build /src/build/install/* /opt/klone/
ENTRYPOINT [ "/opt/klone/bin/klone" ]




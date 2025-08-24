{ pkgs ? import <nixpkgs-unstable> { } }:

pkgs.mkShell {
  buildInputs = [
    (pkgs.gradle.override { java = pkgs.temurin-bin-24; })
    pkgs.temurin-bin-24
  ];
}

{ pkgs ? import <nixpkgs-unstable> { } }:

pkgs.mkShell {
  buildInputs = [
    (pkgs.gradle.override { java = pkgs.jdk17; })
    pkgs.jdk17
  ];
}

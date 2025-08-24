{ pkgs ? import <nixpkgs-unstable> { } }:

pkgs.mkShell {
  buildInputs = [
    (pkgs.gradle.override { java = pkgs.jdk23; })
    pkgs.jdk23
  ];
}

set -e
tag=$1

docker build -t mcorbin/mirabelle-doc:${tag} .
docker push mcorbin/mirabelle-doc:${tag}

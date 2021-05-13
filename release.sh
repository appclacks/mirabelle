set -e
tag=$1
lein codox

git add .
git commit -m "release ${tag}"
git tag -a "${tag}" -m "release ${tag}"
docker build -t mcorbin/mirabelle:${tag} .
docker push mcorbin/mirabelle:${tag}
git push --tags
git push

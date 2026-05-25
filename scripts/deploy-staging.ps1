$ErrorActionPreference = "Stop"

$sha = (git rev-parse --short HEAD).Trim()
$apiImage = "jakeyy8/cloudcommander-api:$sha"
$workerImage = "jakeyy8/cloudcommander-worker:$sha"

Write-Host "Building images for commit $sha..."
docker build -t $apiImage -f Dockerfile.api .
docker build -t $workerImage -f Dockerfile.worker .

Write-Host "Pushing images..."
docker push $apiImage
docker push $workerImage

Write-Host "Patching manifests and deploying..."
(Get-Content k8s/staging/api-deployment.yaml) `
    -replace "image: jakeyy8/cloudcommander-api:.*", "image: $apiImage" |
    Set-Content k8s/staging/api-deployment.yaml

(Get-Content k8s/staging/worker-deployment.yaml) `
    -replace "image: jakeyy8/cloudcommander-worker:.*", "image: $workerImage" |
    Set-Content k8s/staging/worker-deployment.yaml

(Get-Content k8s/staging/migration-job.yaml) `
    -replace "image: jakeyy8/cloudcommander-api:.*", "image: $apiImage" |
    Set-Content k8s/staging/migration-job.yaml

kubectl apply -f k8s/staging/api-deployment.yaml
kubectl apply -f k8s/staging/worker-deployment.yaml

Write-Host "Rollout initiated."
kubectl rollout status deployment/cloudcommander-api -n cloudcommander-staging
kubectl rollout status deployment/cloudcommander-worker -n cloudcommander-staging

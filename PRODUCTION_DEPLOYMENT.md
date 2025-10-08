# Hall of Hate v2 Production Deployment Checklist

## Prerequisites
- [ ] Kubernetes cluster is running
- [ ] kubectl is configured for the cluster
- [ ] Docker image is built and pushed to registry

## Database Setup

### Option 1: Fresh Database (Clean Install)
If deploying to a completely new environment:
```bash
# Apply the updated postgres configuration
kubectl apply -f manifests/postgres/initdb-configmap.yaml
kubectl apply -f manifests/postgres/
```

### Option 2: Existing Database (Migration Required)
If the database already exists in production:
```bash
# Connect to existing postgres pod
kubectl exec -it deployment/postgres -n corderos -- psql -U postgres -d corderos

# Run the migration script
\i /path/to/hall-of-hate-v2-migration.sql

# Or copy and paste the contents of manifests/postgres/hall-of-hate-v2-migration.sql
```

## Storage Setup
```bash
# Apply the Persistent Volume Claim for Hall of Hate images
kubectl apply -f manifests/hall-of-hate-pvc.yaml
```

## Application Deployment
```bash
# Apply all manifests
kubectl apply -f manifests/

# Or apply kustomization
kubectl apply -k manifests/
```

## Verification Steps

### 1. Check Pod Status
```bash
kubectl get pods -n corderos
kubectl logs deployment/corderos-app -n corderos
```

### 2. Verify Database Tables
```bash
kubectl exec -it deployment/postgres -n corderos -- psql -U corderos_app -d corderos -c "\dt hall_of_hate_v2*"
```

### 3. Check Storage Mount
```bash
kubectl exec -it deployment/corderos-app -n corderos -- ls -la /app/app/images/hall_of_hate/uploads/
```

### 4. Test Application
- Access the application via ingress
- Navigate to Hall of Hate section
- Test CRUD operations (Create, Read, Update, Delete)
- Verify image uploads work
- Test rating functionality

## Database Schema Verification

The following tables should exist:
- `hall_of_hate_v2` - Main villains table
- `hall_of_hate_v2_ratings` - User ratings table

Expected schema:
```sql
-- hall_of_hate_v2 table
id SERIAL PRIMARY KEY
name TEXT NOT NULL UNIQUE
image_filename TEXT NOT NULL  
frame_type TEXT DEFAULT 'default'
created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()

-- hall_of_hate_v2_ratings table  
id SERIAL PRIMARY KEY
villain_id INTEGER NOT NULL (FK to hall_of_hate_v2.id)
user_name TEXT NOT NULL
rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 99)
created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
UNIQUE(villain_id, user_name)
```

## Storage Configuration

The production deployment expects:
- PVC: `hall-of-hate-images` (1Gi)
- Mount path: `/app/app/images/hall_of_hate/uploads`
- Access mode: ReadWriteOnce

## Common Issues & Solutions

### Database Connection Issues
- Check DATABASE_URL in configmap
- Verify postgres service is running
- Check network policies

### Storage Issues  
- Verify PVC is bound
- Check volume mount in deployment
- Ensure correct file permissions

### Image Upload Issues
- Check storage mount is working
- Verify file permissions in uploads directory
- Check application logs for path issues

## Rollback Plan
If issues occur:
```bash
# Rollback to previous version
kubectl rollout undo deployment/corderos-app -n corderos

# Check rollout status
kubectl rollout status deployment/corderos-app -n corderos
```
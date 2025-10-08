#!/bin/bash

# Production Database Verification Script
# Run this after deployment to verify Hall of Hate v2 setup

echo "🔍 Verifying Hall of Hate v2 Production Setup..."

# Check if namespace exists
echo "📋 Checking namespace..."
kubectl get namespace corderos || { echo "❌ Namespace 'corderos' not found"; exit 1; }

# Check if postgres pod is running
echo "🗄️  Checking PostgreSQL pod..."
kubectl get pods -n corderos -l app=postgres || { echo "❌ PostgreSQL pod not found"; exit 1; }

# Check if application pod is running  
echo "🚀 Checking application pod..."
kubectl get pods -n corderos -l app=corderos-app || { echo "❌ Application pod not found"; exit 1; }

# Check PVC
echo "💾 Checking Persistent Volume Claim..."
kubectl get pvc hall-of-hate-images -n corderos || { echo "❌ PVC not found"; exit 1; }

# Verify database tables
echo "🏗️  Verifying database schema..."
kubectl exec -n corderos deployment/postgres -- psql -U corderos_app -d corderos -c "\dt hall_of_hate_v2*" || { echo "❌ Database tables not found"; exit 1; }

# Check storage mount
echo "📁 Checking storage mount..."
kubectl exec -n corderos deployment/corderos-app -- ls -la /app/app/images/hall_of_hate/uploads/ || { echo "❌ Storage mount not accessible"; exit 1; }

# Test database connectivity from application
echo "🔗 Testing database connectivity..."
kubectl exec -n corderos deployment/corderos-app -- python -c "
import psycopg2
import os
try:
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM hall_of_hate_v2')
    count = cur.fetchone()[0]
    print(f'✅ Database connection successful. Found {count} villains.')
    conn.close()
except Exception as e:
    print(f'❌ Database connection failed: {e}')
    exit(1)
" || { echo "❌ Database connectivity test failed"; exit 1; }

echo "✅ All checks passed! Hall of Hate v2 is ready for production."
echo ""
echo "🌐 Access your application at: https://your-domain.com/hall-of-hate"
echo "📝 Check logs with: kubectl logs deployment/corderos-app -n corderos"
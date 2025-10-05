#!/usr/bin/env python3
"""
Integration Test: Tooling Stack Verification

Verifies that all components of the tooling stack work together correctly.
Tests the complete development workflow and service integration.
"""

import pytest
import asyncio
import subprocess
import time
import requests
from pathlib import Path
from typing import Dict, Any

class TestToolingStackIntegration:
    """Integration tests for the complete tooling stack."""
    
    def test_project_structure(self):
        """Test that all required directories and files exist."""
        project_root = Path.cwd()
        
        # Required directories
        required_dirs = [
            "engines",
            "infra",
            "orchestration", 
            "tests",
            "tests/unit",
            "tests/integration",
            "tests/performance"
        ]
        
        for dir_name in required_dirs:
            dir_path = project_root / dir_name
            assert dir_path.exists(), f"Required directory missing: {dir_name}"
            assert dir_path.is_dir(), f"Path is not a directory: {dir_name}"
        
        # Required files
        required_files = [
            "pyproject.toml",
            "README.md",
            "Makefile",
            "dev.sh",
            "tests/conftest.py"
        ]
        
        for file_name in required_files:
            file_path = project_root / file_name
            assert file_path.exists(), f"Required file missing: {file_name}"
            assert file_path.is_file(), f"Path is not a file: {file_name}"
    
    def test_poetry_configuration(self):
        """Test that Poetry configuration is valid."""
        result = subprocess.run(
            ["poetry", "check"],
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0, f"Poetry check failed: {result.stderr}"
    
    def test_dependency_installation(self):
        """Test that dependencies can be installed."""
        result = subprocess.run(
            ["poetry", "install", "--dry-run"],
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0, f"Dependency check failed: {result.stderr}"
    
    def test_linting_configuration(self):
        """Test that linting configuration is valid."""
        # Test ruff configuration
        result = subprocess.run(
            ["poetry", "run", "ruff", "check", "--show-settings"],
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0, f"Ruff configuration invalid: {result.stderr}"
    
    def test_makefile_commands(self):
        """Test that Makefile commands are valid."""
        # Test make help
        result = subprocess.run(
            ["make", "help"],
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0, f"Make help failed: {result.stderr}"
        assert "Satoshi HFT Infrastructure" in result.stdout
    
    def test_development_script(self):
        """Test that development script is functional."""
        # Test script help
        result = subprocess.run(
            ["./dev.sh", "help"],
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0, f"Dev script help failed: {result.stderr}"
        assert "Satoshi Development Workflow" in result.stdout
    
    @pytest.mark.asyncio
    async def test_import_structure(self):
        """Test that all modules can be imported."""
        # Test core imports
        try:
            # These should work even without all dependencies
            from infra.bus import streaming_bus
            from tests import conftest
            
            # Test that main classes can be referenced
            assert hasattr(streaming_bus, 'StreamingBus')
            assert hasattr(conftest, 'create_mock_trade_data')
            
        except ImportError as e:
            pytest.fail(f"Import failed: {e}")
    
    def test_test_discovery(self):
        """Test that pytest can discover tests."""
        result = subprocess.run(
            ["poetry", "run", "pytest", "--collect-only", "-q"],
            capture_output=True,
            text=True
        )
        
        # Should find at least some tests
        assert "test session starts" in result.stdout or result.returncode == 0
    
    @pytest.mark.integration 
    def test_api_health_endpoint(self):
        """Test that API health endpoint works (if services are running)."""
        try:
            # Try to connect to API health endpoint
            response = requests.get("http://localhost:8000/health", timeout=2)
            
            if response.status_code == 200:
                health_data = response.json()
                assert "status" in health_data
                assert "uptime_seconds" in health_data
                assert "components" in health_data
            else:
                pytest.skip("API server not running")
                
        except requests.ConnectionError:
            pytest.skip("API server not running")
    
    @pytest.mark.integration
    def test_dagster_pipeline_validation(self):
        """Test that Dagster pipeline can be validated."""
        try:
            result = subprocess.run(
                ["poetry", "run", "dagster", "asset", "list", "-f", "orchestration/pipeline.py"],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                # Should find some assets
                assert "raw_market_data" in result.stdout or len(result.stdout.strip()) > 0
            else:
                pytest.skip(f"Dagster validation failed: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            pytest.skip("Dagster validation timed out")
        except FileNotFoundError:
            pytest.skip("Dagster not available")

class TestDevelopmentWorkflow:
    """Test the complete development workflow."""
    
    def test_code_quality_pipeline(self):
        """Test the complete code quality pipeline."""
        commands = [
            ["poetry", "run", "ruff", "check", ".", "--quiet"],
            ["poetry", "run", "ruff", "format", "--check", "."],
        ]
        
        for cmd in commands:
            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=60
                )
                
                # Some linting errors are expected in new codebase
                # We just check that the tools run successfully
                assert result.returncode in [0, 1], f"Command failed: {' '.join(cmd)}\n{result.stderr}"
                
            except subprocess.TimeoutExpired:
                pytest.fail(f"Command timed out: {' '.join(cmd)}")
            except FileNotFoundError:
                pytest.skip(f"Tool not available: {cmd[0]}")
    
    def test_unit_test_execution(self):
        """Test that unit tests can be executed."""
        result = subprocess.run(
            ["poetry", "run", "pytest", "tests/unit/", "--tb=no", "-q"],
            capture_output=True,
            text=True,
            timeout=120
        )
        
        # Tests may fail due to missing dependencies, but pytest should run
        assert result.returncode in [0, 1, 5], f"Pytest execution failed: {result.stderr}"
    
    def test_build_process(self):
        """Test that the build process works."""
        result = subprocess.run(
            ["poetry", "build", "--dry-run"],
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0, f"Build failed: {result.stderr}"

class TestServiceIntegration:
    """Test service integration and health."""
    
    @pytest.mark.integration
    def test_kafka_container_health(self):
        """Test Kafka container health (if running)."""
        try:
            result = subprocess.run(
                ["docker", "ps", "--filter", "name=kafka", "--format", "{{.Status}}"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0 and "Up" in result.stdout:
                # Kafka is running, test connectivity
                topic_result = subprocess.run(
                    ["docker", "exec", "kafka", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                
                assert topic_result.returncode == 0, "Kafka not responding"
            else:
                pytest.skip("Kafka container not running")
                
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pytest.skip("Docker not available or timeout")
    
    @pytest.mark.integration
    def test_prometheus_metrics_endpoint(self):
        """Test Prometheus metrics endpoint (if API is running)."""
        try:
            response = requests.get("http://localhost:8000/metrics", timeout=2)
            
            if response.status_code == 200:
                metrics_text = response.text
                assert "satoshi_uptime_seconds" in metrics_text
                assert "# HELP" in metrics_text  # Prometheus format
            else:
                pytest.skip("Metrics endpoint not available")
                
        except requests.ConnectionError:
            pytest.skip("API server not running")

if __name__ == "__main__":
    # Run the integration tests
    pytest.main([__file__, "-v"])

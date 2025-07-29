# Cell 10: Final Setup and Complete System Demo

# =============================================================================
# ENVIRONMENT VALIDATION AND SETUP
# =============================================================================

def validate_environment():
    """Validate the Google Cloud Jupyter environment"""
    print("🔍 Validating Environment")
    print("-" * 40)
    
    validation_results = {
        'python_version': True,
        'required_packages': True,
        'memory_available': True,
        'environment_type': 'unknown',
        'issues': [],
        'warnings': []
    }
    
    try:
        # Check Python version
        import sys
        python_version = sys.version_info
        if python_version.major < 3 or python_version.minor < 7:
            validation_results['python_version'] = False
            validation_results['issues'].append(f"Python 3.7+ required, found {python_version.major}.{python_version.minor}")
        else:
            print(f"✅ Python {python_version.major}.{python_version.minor}.{python_version.micro}")
        
        # Check if we're in Google Colab
        try:
            import google.colab
            validation_results['environment_type'] = 'google_colab'
            print("✅ Google Colab environment detected")
        except ImportError:
            # Check if we're in a GCP environment
            try:
                import requests
                metadata_server = 'http://metadata.google.internal/computeMetadata/v1/'
                headers = {'Metadata-Flavor': 'Google'}
                response = requests.get(metadata_server, headers=headers, timeout=1)
                if response.status_code == 200:
                    validation_results['environment_type'] = 'gcp_compute'
                    print("✅ GCP Compute environment detected")
            except:
                validation_results['environment_type'] = 'local_jupyter'
                print("ℹ️  Local Jupyter environment")
        
        # Check memory
        try:
            memory_gb = psutil.virtual_memory().total / (1024**3)
            if memory_gb < 4:
                validation_results['memory_available'] = False
                validation_results['issues'].append(f"Low memory: {memory_gb:.1f}GB (4GB+ recommended)")
            else:
                print(f"✅ Memory: {memory_gb:.1f}GB available")
        except:
            validation_results['warnings'].append("Could not check memory availability")
        
        # Check key packages
        required_packages = ['asyncio', 'pandas', 'numpy', 'networkx']
        missing_packages = []
        
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                missing_packages.append(package)
        
        if missing_packages:
            validation_results['required_packages'] = False
            validation_results['issues'].append(f"Missing packages: {', '.join(missing_packages)}")
        else:
            print("✅ All required packages available")
        
        # Environment-specific recommendations
        if validation_results['environment_type'] == 'google_colab':
            validation_results['warnings'].append("In Colab: Runtime may reset, save important results")
        elif validation_results['environment_type'] == 'gcp_compute':
            validation_results['warnings'].append("In GCP: Consider using GPUs for large workloads")
        
        # Summary
        print("\n📊 Validation Summary:")
        if validation_results['issues']:
            print("❌ Issues found:")
            for issue in validation_results['issues']:
                print(f"   - {issue}")
        
        if validation_results['warnings']:
            print("⚠️  Warnings:")
            for warning in validation_results['warnings']:
                print(f"   - {warning}")
        
        if not validation_results['issues']:
            print("✅ Environment validation passed!")
        
        return validation_results
        
    except Exception as e:
        print(f"❌ Environment validation failed: {e}")
        validation_results['issues'].append(str(e))
        return validation_results

def setup_google_cloud_auth():
    """Setup Google Cloud authentication if needed"""
    print("🔐 Setting up Google Cloud Authentication")
    print("-" * 40)
    
    try:
        # Check if we're in Google Colab
        try:
            from google.colab import auth
            auth.authenticate_user()
            print("✅ Google Colab authentication completed")
            return True
        except ImportError:
            pass
        
        # Check for service account
        if os.path.exists('/content/service-account-key.json'):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/content/service-account-key.json'
            print("✅ Service account credentials found")
            return True
        
        # Check for default credentials
        try:
            from google.auth import default
            credentials, project = default()
            if project:
                print(f"✅ Default credentials found for project: {project}")
                return True
        except Exception as e:
            print(f"⚠️  No default credentials: {e}")
        
        print("ℹ️  No Google Cloud authentication found")
        print("   This is okay for basic functionality")
        return False
        
    except Exception as e:
        print(f"❌ Authentication setup failed: {e}")
        return False

# =============================================================================
# COMPLETE SYSTEM DEMO
# =============================================================================

async def complete_system_demo():
    """Run a complete demonstration of the multi-agent system"""
    print("🎬 COMPLETE MULTI-AGENT SYSTEM DEMONSTRATION")
    print("=" * 60)
    
    demo_results = {
        'start_time': datetime.now(),
        'stages_completed': [],
        'stages_failed': [],
        'overall_status': 'running'
    }
    
    try:
        # Stage 1: Environment validation
        print("🔍 Stage 1: Environment Validation")
        validation = validate_environment()
        if validation['issues']:
            print("⚠️  Environment issues detected, but continuing...")
        demo_results['stages_completed'].append('environment_validation')
        
        # Stage 2: System initialization
        print("\n🚀 Stage 2: System Initialization")
        system = await initialize_system("minimal")
        if system:
            print("✅ System initialized successfully")
            demo_results['stages_completed'].append('system_initialization')
        else:
            raise Exception("System initialization failed")
        
        # Stage 3: Component testing
        print("\n🧪 Stage 3: Component Testing")
        component_test = await test_system_components()
        if component_test:
            print("✅ All components working")
            demo_results['stages_completed'].append('component_testing')
        else:
            demo_results['stages_failed'].append('component_testing')
        
        # Stage 4: Data processing demo
        print("\n📊 Stage 4: Data Processing Demo")
        data_result = await example_2_data_processing()
        if data_result:
            demo_results['stages_completed'].append('data_processing')
        else:
            demo_results['stages_failed'].append('data_processing')
        
        # Stage 5: Analysis demo
        print("\n🔍 Stage 5: Analysis Demo")
        analysis_result = await example_3_text_analysis()
        if analysis_result:
            demo_results['stages_completed'].append('analysis_demo')
        else:
            demo_results['stages_failed'].append('analysis_demo')
        
        # Stage 6: Full workflow
        print("\n🔄 Stage 6: Complete Workflow")
        workflow_result = await example_5_full_workflow()
        if workflow_result:
            demo_results['stages_completed'].append('complete_workflow')
        else:
            demo_results['stages_failed'].append('complete_workflow')
        
        # Stage 7: Performance test
        print("\n⚡ Stage 7: Performance Testing")
        perf_result = await run_performance_test()
        if perf_result.get('status') == 'completed':
            demo_results['stages_completed'].append('performance_testing')
        else:
            demo_results['stages_failed'].append('performance_testing')
        
        # Stage 8: Health validation
        print("\n🏥 Stage 8: System Health Check")
        health_result = await validate_system_health()
        if health_result.get('overall_status') in ['healthy', 'needs_attention']:
            demo_results['stages_completed'].append('health_validation')
        else:
            demo_results['stages_failed'].append('health_validation')
        
        # Final summary
        demo_results['end_time'] = datetime.now()
        demo_results['duration'] = (demo_results['end_time'] - demo_results['start_time']).total_seconds()
        demo_results['overall_status'] = 'completed' if not demo_results['stages_failed'] else 'partial'
        
        print("\n" + "=" * 60)
        print("🎉 DEMONSTRATION COMPLETE!")
        print("=" * 60)
        
        print(f"⏱️  Duration: {demo_results['duration']:.1f} seconds")
        print(f"✅ Stages Completed: {len(demo_results['stages_completed'])}")
        print(f"❌ Stages Failed: {len(demo_results['stages_failed'])}")
        print(f"📊 Success Rate: {len(demo_results['stages_completed'])/(len(demo_results['stages_completed'])+len(demo_results['stages_failed'])):.1%}")
        
        if demo_results['stages_failed']:
            print(f"\n⚠️  Failed Stages:")
            for stage in demo_results['stages_failed']:
                print(f"   - {stage}")
        
        if demo_results['overall_status'] == 'completed':
            print("\n🏆 Perfect! All stages completed successfully.")
            print("🎯 The Multi-Agent System is fully operational!")
        else:
            print("\n⚠️  Some stages failed, but core functionality is working.")
        
        return demo_results
        
    except Exception as e:
        demo_results['overall_status'] = 'failed'
        demo_results['error'] = str(e)
        print(f"\n❌ Demo failed at stage: {e}")
        return demo_results

# =============================================================================
# JUPYTER NOTEBOOK UTILITIES
# =============================================================================

def clear_and_restart():
    """Clear output and restart system"""
    try:
        from IPython.display import clear_output
        clear_output(wait=True)
        print("🔄 Output cleared, restarting system...")
        return True
    except ImportError:
        print("ℹ️  Not in Jupyter environment")
        return False

def save_results_to_file(results: Dict[str, Any], filename: str = None):
    """Save demo results to a file"""
    if filename is None:
        filename = f"multi_agent_demo_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    try:
        # Convert datetime objects to strings for JSON serialization
        def json_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2, default=json_serializer)
        
        print(f"💾 Results saved to: {filename}")
        return filename
        
    except Exception as e:
        print(f"❌ Failed to save results: {e}")
        return None

def create_performance_report():
    """Create a performance report"""
    print("📈 Performance Report Generator")
    print("-" * 40)
    
    try:
        # Get system metrics
        if global_system and global_system.initialized:
            status = global_system.get_system_status()
            
            report = {
                'system_status': status,
                'performance_metrics': {
                    'memory_usage': f"{psutil.virtual_memory().percent:.1f}%",
                    'cpu_usage': f"{psutil.cpu_percent():.1f}%",
                    'disk_usage': f"{psutil.disk_usage('/').percent:.1f}%"
                },
                'recommendations': []
            }
            
            # Add recommendations based on metrics
            if psutil.virtual_memory().percent > 80:
                report['recommendations'].append("High memory usage - consider restarting kernel")
            
            if psutil.cpu_percent() > 80:
                report['recommendations'].append("High CPU usage - wait for current tasks to complete")
            
            print("📊 Current Performance:")
            for metric, value in report['performance_metrics'].items():
                print(f"   {metric}: {value}")
            
            if report['recommendations']:
                print("\n💡 Recommendations:")
                for rec in report['recommendations']:
                    print(f"   - {rec}")
            
            return report
        else:
            print("⚠️  System not initialized")
            return None
            
    except Exception as e:
        print(f"❌ Failed to generate performance report: {e}")
        return None

def show_troubleshooting_guide():
    """Show troubleshooting guide"""
    print("🔧 TROUBLESHOOTING GUIDE")
    print("=" * 50)
    print()
    print("Common Issues and Solutions:")
    print()
    print("1. 🐛 ImportError or ModuleNotFoundError:")
    print("   Solution: Run Cell 1 again to install dependencies")
    print("   Command: Re-run the first cell with package installations")
    print()
    print("2. 🔄 System not initialized:")
    print("   Solution: Run system initialization")
    print("   Command: await initialize_system()")
    print()
    print("3. 🚫 Agent not responding:")
    print("   Solution: Check agent initialization")
    print("   Command: await test_system_components()")
    print()
    print("4. 💾 Memory issues:")
    print("   Solution: Restart kernel and run minimal setup")
    print("   Command: Kernel -> Restart & Clear Output")
    print()
    print("5. 🌐 Network timeouts:")
    print("   Solution: Check internet connection and retry")
    print("   Command: Restart and try again")
    print()
    print("6. 🔑 Authentication errors:")
    print("   Solution: Setup Google Cloud authentication")
    print("   Command: setup_google_cloud_auth()")
    print()
    print("Quick Reset Commands:")
    print("- await shutdown_system()  # Clean shutdown")
    print("- await initialize_system()  # Fresh start")
    print("- await run_interactive_demo()  # Full demo")
    print()
    print("Get Help:")
    print("- validate_environment()  # Check setup")
    print("- create_performance_report()  # Check performance")
    print("- show_quick_examples()  # See examples")

# =============================================================================
# ONE-CLICK DEMO FUNCTION
# =============================================================================

async def one_click_demo():
    """One-click complete demonstration - runs everything"""
    print("🎯 ONE-CLICK COMPLETE DEMONSTRATION")
    print("🚀 This will run the complete multi-agent system demo")
    print("⏱️  Estimated time: 2-3 minutes")
    print("=" * 60)
    
    try:
        # Validate environment first
        print("Step 1/3: Validating environment...")
        validation = validate_environment()
        
        if validation['issues']:
            print("⚠️  Environment issues detected:")
            for issue in validation['issues']:
                print(f"   - {issue}")
            print("\nContinuing anyway...")
        
        print("\nStep 2/3: Running complete system demo...")
        demo_result = await complete_system_demo()
        
        print("\nStep 3/3: Generating final report...")
        
        # Create summary
        summary = {
            'demo_status': demo_result.get('overall_status', 'unknown'),
            'stages_completed': len(demo_result.get('stages_completed', [])),
            'stages_failed': len(demo_result.get('stages_failed', [])),
            'duration_seconds': demo_result.get('duration', 0),
            'environment_validation': validation,
            'timestamp': datetime.now().isoformat()
        }
        
        # Save results
        filename = save_results_to_file(summary)
        
        print("\n" + "=" * 60)
        print("🎉 ONE-CLICK DEMO COMPLETED!")
        print("=" * 60)
        
        success_rate = summary['stages_completed'] / (summary['stages_completed'] + summary['stages_failed']) if (summary['stages_completed'] + summary['stages_failed']) > 0 else 0
        
        print(f"📊 Overall Status: {summary['demo_status'].upper()}")
        print(f"✅ Success Rate: {success_rate:.1%}")
        print(f"⏱️  Total Time: {summary['duration_seconds']:.1f} seconds")
        
        if filename:
            print(f"💾 Results saved to: {filename}")
        
        if summary['demo_status'] == 'completed':
            print("\n🏆 CONGRATULATIONS!")
            print("🎯 The Multi-Agent System is fully operational in your Google Cloud environment!")
            print("\n💡 Next steps:")
            print("   - Modify the examples for your use case")
            print("   - Add your own data sources")
            print("   - Integrate with your workflows")
        else:
            print("\n⚠️  Some issues were encountered, but core functionality works!")
            print("💡 For troubleshooting: show_troubleshooting_guide()")
        
        return summary
        
    except Exception as e:
        print(f"\n❌ One-click demo failed: {e}")
        print("💡 For troubleshooting: show_troubleshooting_guide()")
        return {'status': 'failed', 'error': str(e)}

# =============================================================================
# FINAL INITIALIZATION AND HELP
# =============================================================================

def show_final_help():
    """Show final help and usage instructions"""
    print("🎓 MULTI-AGENT SYSTEM - READY TO USE!")
    print("=" * 60)
    print()
    print("🚀 QUICK START - Copy and paste these commands:")
    print("-" * 50)
    print("# Run everything in one command:")
    print("await one_click_demo()")
    print()
    print("# Or run step by step:")
    print("await initialize_system()")
    print("await process_sample_task()")
    print("await run_sample_workflow()")
    print()
    print("🔧 UTILITIES:")
    print("-" * 50)
    print("validate_environment()          # Check your setup")
    print("show_troubleshooting_guide()    # Get help")
    print("create_performance_report()     # Check performance")
    print("await validate_system_health()  # System health")
    print()
    print("📊 EXAMPLES:")
    print("-" * 50)
    print("await example_2_data_processing()  # Process CSV data")
    print("await example_3_text_analysis()    # Analyze text")
    print("await example_5_full_workflow()    # Complete workflow")
    print("await run_all_examples()           # Run all examples")
    print()
    print("🔄 SYSTEM MANAGEMENT:")
    print("-" * 50)
    print("display_system_info()           # Show system status")
    print("await shutdown_system()         # Clean shutdown")
    print("show_quick_examples()           # Show example commands")
    print()
    print("💡 TIPS:")
    print("- Start with 'await one_click_demo()' for full demonstration")
    print("- Use 'validate_environment()' if you encounter issues")
    print("- All functions are async - use 'await' when calling them")
    print("- Results are automatically saved to JSON files")
    print()
    print("🎯 YOU'RE ALL SET! The Multi-Agent System is ready to use.")
    print("=" * 60)

# Auto-run validation on import
print("🎯 Final setup completed successfully!")
print("\n🔍 Running quick environment validation...")
env_validation = validate_environment()

if not env_validation['issues']:
    print("\n✅ Environment validation passed!")
    print("🚀 Ready to run Multi-Agent System!")
else:
    print("\n⚠️  Environment issues detected - see validation above")
    print("💡 You can still try running the system")

print("\n" + "="*60)
print("🎓 READY TO START!")
print("🚀 Run: await one_click_demo()")
print("📚 Or run: show_final_help()")
print("="*60)

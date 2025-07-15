# 🔧 Kestra runIf Condition Debugging Guide

## 🚨 **COMMON RUNIF ERRORS**

### **Error: "Unable to find `stdout` used in the expression"**
```
ERROR Failed to evaluate the runIf condition for task update_macro_data. 
Cause: Unable to find `stdout` used in the expression 
`{{ outputs.check_ingestion_status.stdout | trim | startsWith('INGESTION_COMPLETE') }}`
```

**Root Cause**: The referenced task failed or didn't produce stdout output.

## 🔍 **DEBUGGING STEPS**

### **1. Check Task Execution Status**
In Kestra UI:
1. Go to Executions → Your failed execution
2. Check if the referenced task (`check_ingestion_status`) completed successfully
3. Look for the task's exit code and output

### **2. Verify Task Output**
```yaml
# Task must exit with code 0 to capture stdout
command:
  - "sh"
  - "-c"
  - |
    echo "This will be captured as stdout"
    # Always exit 0 for stdout capture
    exit 0
```

### **3. Common runIf Patterns**

#### **Check Task Success**
```yaml
runIf: "{{ outputs.previous_task.exitCode == 0 }}"
```

#### **Check Stdout Content**
```yaml
runIf: "{{ outputs.previous_task.stdout | trim | startsWith('SUCCESS') }}"
```

#### **Check File Existence (via stdout)**
```yaml
# In the checking task:
command:
  - "sh"
  - "-c"
  - |
    if [ -f "/path/to/file" ]; then
        echo "FILE_EXISTS"
    else
        echo "FILE_MISSING"
    fi
    exit 0  # Always exit 0

# In the dependent task:
runIf: "{{ outputs.check_task.stdout | trim | startsWith('FILE_EXISTS') }}"
```

## ✅ **FIXED PATTERNS IN CANDLETHROB**

### **Before (Problematic)**
```yaml
command:
  - "sh"
  - "-c"
  - |
    if [ -f /app/data/flag.file ]; then
        echo 'SUCCESS'
    else
        echo 'FAILED'
        exit 1  # ❌ This prevents stdout capture
    fi
```

### **After (Fixed)**
```yaml
command:
  - "sh"
  - "-c"
  - |
    echo "Checking status..."
    if [ -f /app/data/flag.file ]; then
        echo 'INGESTION_COMPLETE'
        echo "Flag file found, operation completed"
    else
        echo 'INGESTION_INCOMPLETE'
        echo "Flag file not found, operation may have failed"
    fi
    # ✅ Always exit 0 so stdout is captured
    exit 0
```

## 🛠️ **BEST PRACTICES**

### **1. Always Capture Output**
- Use `exit 0` in status check tasks
- Provide meaningful stdout messages
- Include debugging information

### **2. Robust Condition Checking**
```yaml
# Check multiple conditions
runIf: "{{ outputs.task1.exitCode == 0 and outputs.task2.stdout | trim | startsWith('SUCCESS') }}"

# Use default values
runIf: "{{ (outputs.task1.stdout | default('FAILED')) | trim | startsWith('SUCCESS') }}"
```

### **3. Error Handling**
```yaml
# Graceful degradation
runIf: "{{ outputs.check_task.exitCode == 0 and (outputs.check_task.stdout | default('') | trim | startsWith('COMPLETE')) }}"
```

### **4. Debugging Output**
```yaml
command:
  - "sh"
  - "-c"
  - |
    echo "=== Status Check Debug ==="
    echo "Timestamp: $(date)"
    echo "Working directory: $(pwd)"
    echo "Files in data directory:"
    ls -la /app/data/ 2>/dev/null || echo "Data directory not accessible"
    
    if [ -f /app/data/completion.flag ]; then
        echo "STATUS: COMPLETE"
        echo "Flag content:"
        cat /app/data/completion.flag
    else
        echo "STATUS: INCOMPLETE"
        echo "Flag file not found"
    fi
    echo "=== End Debug ==="
    exit 0
```

## 🎯 **TESTING RUNIF CONDITIONS**

### **Manual Testing**
1. Run the prerequisite task manually
2. Check its output in Kestra UI
3. Test the runIf expression in a simple task
4. Verify the condition evaluates correctly

### **Expression Testing**
```yaml
# Test task to verify expressions
- id: test_expression
  type: io.kestra.plugin.core.log.Log
  message: "Expression result: {{ outputs.previous_task.stdout | trim | startsWith('SUCCESS') }}"
```

## 🚀 **CURRENT CANDLETHROB STATUS**

All CandleThrob flows have been updated with robust runIf conditions:

- ✅ Status check tasks always exit 0
- ✅ Comprehensive stdout output
- ✅ Debugging information included
- ✅ Graceful handling of missing files
- ✅ Clear success/failure indicators

The runIf condition errors should now be resolved!

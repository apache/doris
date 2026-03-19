#!/usr/bin/env python3

import sys
import re

def fix_imports(filename):
    with open(filename, 'r') as f:
        content = f.read()
    
    # Find all imports
    import_lines = []
    other_lines = []
    in_import_section = False
    
    for line in content.split('\n'):
        stripped = line.strip()
        if stripped.startswith('import ') and stripped.endswith(';'):
            in_import_section = True
            import_lines.append(line)
        elif stripped.startswith('package ') or stripped == '' or stripped.startswith('//'):
            other_lines.append(line)
        elif in_import_section and stripped == '':
            # End of import section
            other_lines.append(line)
        elif in_import_section:
            # Start of code after imports
            in_import_section = False
            other_lines.append(line)
        else:
            other_lines.append(line)
    
    # Separate imports into groups
    doris_imports = []
    third_party_imports = []
    java_imports = []
    
    for imp in import_lines:
        if 'org.apache.doris' in imp:
            doris_imports.append(imp)
        elif 'java.' in imp or 'javax.' in imp:
            java_imports.append(imp)
        else:
            third_party_imports.append(imp)
    
    # Sort each group
    doris_imports.sort()
    third_party_imports.sort()
    java_imports.sort()
    
    # Reconstruct the file
    result_lines = []
    for line in other_lines:
        if line.strip().startswith('import ') and line.strip().endswith(';'):
            continue
        elif 'import com.google.common.base.Objects;' in line:
            # Insert sorted imports here
            for imp in doris_imports:
                result_lines.append(imp)
            result_lines.append('')
            for imp in third_party_imports:
                result_lines.append(imp)
            for imp in java_imports:
                result_lines.append(imp)
            result_lines.append('')
        else:
            result_lines.append(line)
    
    with open(filename, 'w') as f:
        f.write('\n'.join(result_lines))

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python fix_imports.py <filename>")
        sys.exit(1)
    
    fix_imports(sys.argv[1])
"""
Sensitive Content Detector
Analyzes file names and paths to identify potentially sensitive content
"""

import re
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum


class SensitivityLevel(Enum):
    """Sensitivity levels for content classification"""
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class SensitivityPattern:
    """Pattern definition for sensitive content detection"""
    pattern: str
    level: SensitivityLevel
    category: str
    description: str
    case_sensitive: bool = False


class SensitiveContentDetector:
    """Detects potentially sensitive content based on file names and paths"""

    # Define patterns for sensitive content detection
    SENSITIVITY_PATTERNS = [
        # Authentication & Security
        SensitivityPattern(r'password', SensitivityLevel.CRITICAL, 'auth', 'Password file'),
        SensitivityPattern(r'credential', SensitivityLevel.CRITICAL, 'auth', 'Credential file'),
        SensitivityPattern(r'secret', SensitivityLevel.CRITICAL, 'auth', 'Secret/key file'),
        SensitivityPattern(r'private.*key', SensitivityLevel.CRITICAL, 'auth', 'Private key file'),
        SensitivityPattern(r'\.pem$', SensitivityLevel.CRITICAL, 'auth', 'Certificate file'),
        SensitivityPattern(r'\.pfx$', SensitivityLevel.CRITICAL, 'auth', 'Certificate file'),
        SensitivityPattern(r'token', SensitivityLevel.HIGH, 'auth', 'Token file'),
        SensitivityPattern(r'auth', SensitivityLevel.HIGH, 'auth', 'Authentication file'),

        # Financial
        SensitivityPattern(r'w[29][-_]', SensitivityLevel.CRITICAL, 'financial', 'Tax form (W2/W9)'),
        SensitivityPattern(r'w-?[29][-_\s]20\d{2}', SensitivityLevel.CRITICAL, 'financial', 'Tax form with year'),
        SensitivityPattern(r'1099', SensitivityLevel.CRITICAL, 'financial', 'Tax form 1099'),
        SensitivityPattern(r'tax', SensitivityLevel.HIGH, 'financial', 'Tax document'),
        SensitivityPattern(r'payroll', SensitivityLevel.CRITICAL, 'financial', 'Payroll data'),
        SensitivityPattern(r'salary', SensitivityLevel.CRITICAL, 'financial', 'Salary information'),
        SensitivityPattern(r'compensation', SensitivityLevel.HIGH, 'financial', 'Compensation data'),
        SensitivityPattern(r'invoice', SensitivityLevel.MEDIUM, 'financial', 'Invoice'),
        SensitivityPattern(r'bank.*statement', SensitivityLevel.CRITICAL, 'financial', 'Bank statement'),
        SensitivityPattern(r'financial.*statement', SensitivityLevel.HIGH, 'financial', 'Financial statement'),

        # Legal & Contracts
        SensitivityPattern(r'legal', SensitivityLevel.HIGH, 'legal', 'Legal document'),
        SensitivityPattern(r'contract', SensitivityLevel.HIGH, 'legal', 'Contract'),
        SensitivityPattern(r'agreement', SensitivityLevel.HIGH, 'legal', 'Agreement'),
        SensitivityPattern(r'nda', SensitivityLevel.HIGH, 'legal', 'Non-disclosure agreement'),
        SensitivityPattern(r'confidential', SensitivityLevel.HIGH, 'legal', 'Confidential document'),
        SensitivityPattern(r'proprietary', SensitivityLevel.HIGH, 'legal', 'Proprietary information'),
        SensitivityPattern(r'_signed', SensitivityLevel.HIGH, 'legal', 'Signed document'),
        SensitivityPattern(r'litigation', SensitivityLevel.CRITICAL, 'legal', 'Litigation document'),

        # Personal Information
        SensitivityPattern(r'ssn', SensitivityLevel.CRITICAL, 'pii', 'Social Security Number'),
        SensitivityPattern(r'social.*security', SensitivityLevel.CRITICAL, 'pii', 'Social Security info'),
        SensitivityPattern(r'driver.*license', SensitivityLevel.HIGH, 'pii', 'Driver license'),
        SensitivityPattern(r'passport', SensitivityLevel.HIGH, 'pii', 'Passport'),
        SensitivityPattern(r'birth.*certificate', SensitivityLevel.HIGH, 'pii', 'Birth certificate'),
        SensitivityPattern(r'medical', SensitivityLevel.CRITICAL, 'pii', 'Medical information'),
        SensitivityPattern(r'health', SensitivityLevel.HIGH, 'pii', 'Health information'),
        SensitivityPattern(r'patient', SensitivityLevel.CRITICAL, 'pii', 'Patient data'),
        SensitivityPattern(r'employee.*id', SensitivityLevel.HIGH, 'pii', 'Employee ID'),

        # HR & Employment
        SensitivityPattern(r'resume', SensitivityLevel.MEDIUM, 'hr', 'Resume/CV'),
        SensitivityPattern(r'cv[-_\s]', SensitivityLevel.MEDIUM, 'hr', 'Curriculum Vitae'),
        SensitivityPattern(r'application', SensitivityLevel.MEDIUM, 'hr', 'Application'),
        SensitivityPattern(r'performance.*review', SensitivityLevel.HIGH, 'hr', 'Performance review'),
        SensitivityPattern(r'disciplinary', SensitivityLevel.HIGH, 'hr', 'Disciplinary action'),
        SensitivityPattern(r'termination', SensitivityLevel.HIGH, 'hr', 'Termination document'),
        SensitivityPattern(r'onboarding', SensitivityLevel.MEDIUM, 'hr', 'Onboarding document'),

        # Business Sensitive
        SensitivityPattern(r'strategy', SensitivityLevel.HIGH, 'business', 'Strategic document'),
        SensitivityPattern(r'roadmap', SensitivityLevel.HIGH, 'business', 'Product/Business roadmap'),
        SensitivityPattern(r'acquisition', SensitivityLevel.CRITICAL, 'business', 'Acquisition document'),
        SensitivityPattern(r'merger', SensitivityLevel.CRITICAL, 'business', 'Merger document'),
        SensitivityPattern(r'board.*meeting', SensitivityLevel.HIGH, 'business', 'Board meeting'),
        SensitivityPattern(r'executive.*summary', SensitivityLevel.HIGH, 'business', 'Executive summary'),

        # Development & Technical
        SensitivityPattern(r'\.env$', SensitivityLevel.CRITICAL, 'technical', 'Environment configuration'),
        SensitivityPattern(r'config.*prod', SensitivityLevel.HIGH, 'technical', 'Production config'),
        SensitivityPattern(r'api.*key', SensitivityLevel.CRITICAL, 'technical', 'API key'),
        SensitivityPattern(r'backup', SensitivityLevel.HIGH, 'technical', 'Backup file'),
        SensitivityPattern(r'database.*dump', SensitivityLevel.CRITICAL, 'technical', 'Database dump'),

        # General Sensitive Indicators
        SensitivityPattern(r'sensitive', SensitivityLevel.HIGH, 'general', 'Marked as sensitive'),
        SensitivityPattern(r'restricted', SensitivityLevel.HIGH, 'general', 'Restricted access'),
        SensitivityPattern(r'internal.*only', SensitivityLevel.HIGH, 'general', 'Internal only'),
        SensitivityPattern(r'do.*not.*share', SensitivityLevel.HIGH, 'general', 'Do not share'),
        SensitivityPattern(r'draft', SensitivityLevel.MEDIUM, 'general', 'Draft document'),
    ]

    def __init__(self):
        """Initialize the detector with compiled regex patterns"""
        self.compiled_patterns: List[Tuple[re.Pattern, SensitivityPattern]] = []
        for pattern in self.SENSITIVITY_PATTERNS:
            flags = 0 if pattern.case_sensitive else re.IGNORECASE
            try:
                compiled = re.compile(pattern.pattern, flags)
                self.compiled_patterns.append((compiled, pattern))
            except re.error:
                # Skip invalid patterns
                continue

    def analyze_file_name(self, file_name: str, file_path: Optional[str] = None) -> Dict[str, any]:
        """
        Analyze a file name and path for sensitive content indicators

        Args:
            file_name: The name of the file
            file_path: Optional full path to the file

        Returns:
            Dictionary containing:
                - sensitivity_score: Numeric score (0-100)
                - sensitivity_level: SensitivityLevel enum
                - matched_patterns: List of matched patterns
                - categories: Set of matched categories
                - risk_factors: List of risk factor descriptions
        """
        matched_patterns = []
        categories = set()
        risk_factors = []
        max_level = SensitivityLevel.LOW

        # Analyze file name and path
        text_to_analyze = file_name.lower()
        if file_path:
            text_to_analyze += " " + file_path.lower()

        # Check against all patterns
        for compiled_pattern, pattern_info in self.compiled_patterns:
            if compiled_pattern.search(text_to_analyze):
                matched_patterns.append(pattern_info)
                categories.add(pattern_info.category)
                risk_factors.append(pattern_info.description)
                if pattern_info.level.value > max_level.value:
                    max_level = pattern_info.level

        # Calculate sensitivity score (0-100)
        base_score = max_level.value * 20  # 20, 40, 60, or 80

        # Add bonus for multiple matches
        if len(matched_patterns) > 1:
            base_score += min(len(matched_patterns) * 5, 20)

        # Add bonus for multiple categories
        if len(categories) > 1:
            base_score += min(len(categories) * 5, 15)

        # Cap at 100
        sensitivity_score = min(base_score, 100)

        return {
            'sensitivity_score': sensitivity_score,
            'sensitivity_level': max_level,
            'matched_patterns': matched_patterns,
            'categories': list(categories),
            'risk_factors': risk_factors,
            'is_sensitive': sensitivity_score >= 40  # Consider sensitive if score >= 40
        }

    def batch_analyze(self, file_names: List[str]) -> Dict[str, Dict[str, any]]:
        """
        Analyze multiple file names in batch

        Args:
            file_names: List of file names to analyze

        Returns:
            Dictionary mapping file names to their analysis results
        """
        results = {}
        for file_name in file_names:
            results[file_name] = self.analyze_file_name(file_name)
        return results

    def get_sensitivity_level_name(self, level: SensitivityLevel) -> str:
        """Get human-readable name for sensitivity level"""
        return {
            SensitivityLevel.LOW: "Low",
            SensitivityLevel.MEDIUM: "Medium",
            SensitivityLevel.HIGH: "High",
            SensitivityLevel.CRITICAL: "Critical"
        }.get(level, "Unknown")

    def get_sensitivity_color(self, level: SensitivityLevel) -> str:
        """Get color code for sensitivity level (for UI display)"""
        return {
            SensitivityLevel.LOW: "#10b981",      # Green
            SensitivityLevel.MEDIUM: "#f59e0b",   # Yellow
            SensitivityLevel.HIGH: "#ef4444",     # Red
            SensitivityLevel.CRITICAL: "#991b1b"  # Dark Red
        }.get(level, "#6b7280")  # Gray default

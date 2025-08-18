import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from data_scanner.data_profiler import DataProfiler
from data_scanner.data_quality import DataQualityChecker

def show_data_profiler():
    """Main function to display the data profiler page"""
    
    # st.title("Data Profiler & Quality Assessment")
    
    # Description section
    # st.markdown("""
    
    # ### 📊 **Profiling Capabilities:**
    # - **Basic Statistics**: Row/column counts, data types, memory usage analysis
    # - **Missing Value Analysis**: Identifies and quantifies missing data patterns
    # - **Uniqueness Assessment**: Detects potential primary keys and duplicate records
    # - **Data Type Analysis**: Distribution and consistency of different data types
    # - **Statistical Summaries**: Comprehensive analysis for numeric columns
    
    # ### 🔍 **Quality Checks Performed:**
    # - **Missing Data Detection**: Percentage and patterns of missing values across columns
    # - **Duplicate Row Identification**: Finds exact duplicate records in your data
    # - **Outlier Detection**: Uses IQR (Interquartile Range) method for numeric columns
    # - **Data Type Consistency**: Identifies mixed or inconsistent data types
    # - **Completeness Assessment**: Overall data completeness scoring (0-100 scale)
    # - **Quality Scoring**: Comprehensive quality score with detailed breakdown
    
    # ### 💡 **Smart Recommendations:**
    # - **Actionable Insights**: Specific recommendations for data quality improvements
    # - **Priority-based**: Focuses on most critical issues first
    # - **Business-friendly**: Clear explanations for non-technical stakeholders
    # """)
    
    # st.divider()
    
    # Initialize the profiler and quality checker
    try:
        from utils import get_schema
        schema = get_schema()
        profiler = DataProfiler(schema)
        quality_checker = DataQualityChecker(schema)
        
        available_tables = list(schema.keys()) if schema else []
        
        if not available_tables:
            st.error("❌ No database tables found. Please check your database connection.")
            return
        
        # Table selection
        st.subheader("Select Table for Analysis")
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            selected_table = st.selectbox(
                "Choose a table to profile:",
                options=available_tables,
                help="Select a table from your database to perform comprehensive data profiling and quality assessment"
            )
        
        with col2:
            st.markdown("<br>", unsafe_allow_html=True)  # Add some space
            run_profiling = st.button(
                "Start Profiling", 
                type="primary",
                help="Click to begin comprehensive data analysis"
            )
        
        # Show table schema info
        if selected_table:
            st.markdown(f"**Selected Table:** `{selected_table}`")
            with st.expander("View Table Schema"):
                schema_info = schema.get(selected_table, [])
                if schema_info:
                    schema_df = pd.DataFrame({"Column Name": schema_info})
                    st.dataframe(schema_df, use_container_width=True)
                else:
                    st.info("Schema information not available for this table.")
        
        # Run profiling when button is clicked
        if run_profiling and selected_table:
            with st.spinner(f"🔍 Analyzing table: {selected_table}... This may take a moment."):
                # Generate profiling report
                profile_result = profiler.profile_table(selected_table)
                quality_issues = quality_checker.run_quality_checks()
                
                # Store results in session state to persist across reruns
                st.session_state['profile_result'] = profile_result
                st.session_state['quality_issues'] = quality_issues
                st.session_state['profiled_table'] = selected_table
                
        # Check if we have profiling results to display (either from current run or session state)
        if 'profile_result' in st.session_state and st.session_state.get('profiled_table') == selected_table:
            profile_result = st.session_state['profile_result']
            quality_issues = st.session_state['quality_issues']
            
            # Display results
            st.success(f"✅ Profiling completed for table: **{selected_table}**")
            
            # Use full width container for results
            with st.container():
                # Quality Score Dashboard
                st.subheader("🎯 Overall Quality Score")
                
                # Calculate quality score
                quality_score = calculate_quality_score(profile_result, quality_issues)
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric(
                        "Quality Score", 
                        f"{quality_score['score']}/100",
                        help="Overall data quality score (0-100)"
                    )
                with col2:
                    st.metric(
                        "Category", 
                        f"{quality_score['category']}",
                        help="Quality category based on score"
                    )
                with col3:
                    st.metric(
                        "Issues Found", 
                        len(quality_issues),
                        help="Number of quality issues detected"
                    )
                with col4:
                    st.metric(
                        "Data Size", 
                        f"{profile_result.get('row_count', 0):,} rows",
                        help="Total number of records analyzed"
                    )
                
                st.markdown("---")  # Add separator
                
                # Basic Statistics
                st.subheader("📋 Basic Statistics")            # Get column profiles for statistics
            column_profiles = profile_result.get('column_profiles', {})
            total_columns = len(column_profiles)
            
            # Count column types based on the data_type from profiling
            numeric_cols = len([col for col, profile in column_profiles.items() 
                              if isinstance(profile, dict) and profile.get('data_type') in ['numeric', 'integer']])
            text_cols = len([col for col, profile in column_profiles.items() 
                           if isinstance(profile, dict) and profile.get('data_type') in ['string', 'unknown']])
            datetime_cols = len([col for col, profile in column_profiles.items() 
                               if isinstance(profile, dict) and profile.get('data_type') in ['datetime']])
            boolean_cols = len([col for col, profile in column_profiles.items() 
                              if isinstance(profile, dict) and profile.get('data_type') in ['boolean']])
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Rows", f"{profile_result.get('row_count', 0):,}")
            with col2:
                st.metric("Total Columns", total_columns)
            with col3:
                st.metric("Numeric Columns", numeric_cols)
            with col4:
                st.metric("Text Columns", text_cols)
            
            # Data Quality Analysis
            st.subheader("🔍 Data Quality Issues")
            
            if quality_issues:
                for issue in quality_issues:
                    issue_type = issue.get('type', 'Unknown')
                    severity = issue.get('severity', 'Medium')
                    message = issue.get('message', 'No description available')
                    
                    if severity == 'High':
                        st.error(f"🔴 **{issue_type}**: {message}")
                    elif severity == 'Medium':
                        st.warning(f"🟡 **{issue_type}**: {message}")
                    else:
                        st.info(f"🟢 **{issue_type}**: {message}")
            else:
                st.success("✅ No data quality issues detected!")
            
            # Detailed Column Analysis
            with st.expander("📋 Detailed Column Analysis", expanded=True):
                if 'columns' in profile_result:
                    column_data = []
                    for col_name, col_info in profile_result['columns'].items():
                        column_data.append({
                            'Column': col_name,
                            'Data Type': col_info.get('type', 'Unknown'),
                            'Null Count': col_info.get('null_count', 0),
                            'Null %': f"{col_info.get('null_percentage', 0):.1f}%",
                            'Unique Values': col_info.get('unique_count', 0),
                            'Mean': col_info.get('mean', 'N/A'),
                            'Std Dev': col_info.get('std', 'N/A')
                        })
                    
                    column_df = pd.DataFrame(column_data)
                    st.dataframe(column_df, use_container_width=True)
            
            # Anomalies Analysis with Interactive Data Viewing
            if 'anomalies' in profile_result and profile_result['anomalies']:
                # Filter out reviewed outliers
                unreviewed_anomalies = []
                for anomaly in profile_result['anomalies']:
                    column_name = anomaly.get('column', 'Unknown')
                    table_key = f"{selected_table}_{column_name}"
                    
                    # Only show anomalies that haven't been marked as reviewed
                    if not st.session_state.get('reviewed_outliers', {}).get(table_key, False):
                        unreviewed_anomalies.append(anomaly)
                
                if unreviewed_anomalies:
                    with st.expander("⚠️ Anomalies Detected", expanded=True):
                        st.write("**Statistical outliers detected in your data. Click buttons below to view the actual records:**")
                        
                        for i, anomaly in enumerate(unreviewed_anomalies):
                            anomaly_type = anomaly.get('type', 'Unknown')
                            column_name = anomaly.get('column', 'Unknown')
                            message = anomaly.get('message', 'Anomaly detected')
                            
                            # Display anomaly info with button
                            col1, col2 = st.columns([3, 1])
                            with col1:
                                st.warning(f"• {message}")
                            with col2:
                                button_key = f"show_outliers_{i}_{column_name}"
                                if st.button(f"🔍 View Data", key=button_key, help=f"Show outlier records for {column_name}"):
                                    # Store outlier display request in session state
                                    st.session_state[f'show_outliers_{column_name}'] = True
                                    
                        # Check session state for outlier display requests
                        for i, anomaly in enumerate(unreviewed_anomalies):
                            anomaly_type = anomaly.get('type', 'Unknown')
                            column_name = anomaly.get('column', 'Unknown')
                            
                            # Show outlier data if requested
                            if st.session_state.get(f'show_outliers_{column_name}', False):
                                if anomaly_type == 'STATISTICAL_OUTLIERS':
                                    show_statistical_outliers(selected_table, column_name)
                                    
                                    # Add buttons to hide or mark as reviewed
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        if st.button(f"❌ Hide {column_name} Outliers", key=f"hide_outliers_{column_name}"):
                                            st.session_state[f'show_outliers_{column_name}'] = False
                                            st.rerun()
                                    with col2:
                                        if st.button(f"✅ Reviewed & Looks Good", key=f"reviewed_outliers_{column_name}", 
                                                   help="Mark these outliers as reviewed - they won't appear in future analysis"):
                                            # Mark outliers as reviewed for this table and column
                                            if 'reviewed_outliers' not in st.session_state:
                                                st.session_state['reviewed_outliers'] = {}
                                            
                                            table_key = f"{selected_table}_{column_name}"
                                            st.session_state['reviewed_outliers'][table_key] = True
                                            st.session_state[f'show_outliers_{column_name}'] = False
                                            
                                            st.success(f"✅ Outliers for {column_name} marked as reviewed!")
                                            st.rerun()
                else:
                    # Check if there are reviewed outliers to show status
                    reviewed_count = len([a for a in profile_result['anomalies'] 
                                        if st.session_state.get('reviewed_outliers', {}).get(f"{selected_table}_{a.get('column', '')}", False)])
                    if reviewed_count > 0:
                        st.success(f"✅ All anomalies reviewed! ({reviewed_count} outlier columns marked as acceptable)")
                        
                        # Add option to manage reviewed outliers
                        with st.expander("🔧 Manage Reviewed Outliers", expanded=False):
                            st.write("**Previously reviewed outlier columns:**")
                            reviewed_cols = [a.get('column', '') for a in profile_result['anomalies'] 
                                           if st.session_state.get('reviewed_outliers', {}).get(f"{selected_table}_{a.get('column', '')}", False)]
                            
                            for col in reviewed_cols:
                                col1, col2 = st.columns([3, 1])
                                with col1:
                                    st.info(f"📊 {col} - marked as acceptable")
                                with col2:
                                    if st.button(f"🔄 Re-analyze", key=f"reset_{col}", help=f"Remove review status for {col}"):
                                        table_key = f"{selected_table}_{col}"
                                        if 'reviewed_outliers' in st.session_state:
                                            st.session_state['reviewed_outliers'].pop(table_key, None)
                                        st.rerun()
                            
                            st.markdown("---")
                            if st.button("🔄 Reset All Reviews for This Table", 
                                       help="Remove all review statuses for this table's outliers"):
                                if 'reviewed_outliers' in st.session_state:
                                    # Remove all reviewed outliers for this table
                                    keys_to_remove = [k for k in st.session_state['reviewed_outliers'].keys() 
                                                     if k.startswith(f"{selected_table}_")]
                                    for key in keys_to_remove:
                                        st.session_state['reviewed_outliers'].pop(key, None)
                                st.success("All review statuses reset!")
                                st.rerun()
                    else:
                        st.success("✅ No statistical anomalies detected!")
            else:
                st.success("✅ No statistical anomalies detected!")
            
            # Missing Values Visualization
            if 'columns' in profile_result:
                missing_data = []
                for col_name, col_info in profile_result['columns'].items():
                    if col_info.get('null_count', 0) > 0:
                        missing_data.append({
                            'Column': col_name,
                            'Missing Count': col_info.get('null_count', 0),
                            'Missing %': col_info.get('null_percentage', 0)
                        })
                
                if missing_data:
                    st.subheader("📊 Missing Values Visualization")
                    missing_df = pd.DataFrame(missing_data)
                    
                    fig = px.bar(
                        missing_df, 
                        x='Column', 
                        y='Missing Count',
                        title="Missing Values by Column",
                        hover_data=['Missing %']
                    )
                    fig.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig, use_container_width=True)
            
            # Recommendations
            st.subheader("💡 Recommendations")
            recommendations = generate_recommendations(profile_result, quality_issues)
            
            for i, rec in enumerate(recommendations, 1):
                st.markdown(f"**{i}.** {rec}")
                
        else:
            # No profiling results available - show instruction
            st.info("**Select a table and click 'Start Profiling' to begin analysis.**")
            
    except ImportError as e:
        st.error(f"❌ Required modules not found: {e}")
        st.info("Please ensure your data_scanner modules are properly configured.")
    except Exception as e:
        st.error(f"❌ An error occurred: {e}")

def calculate_quality_score(profile_result, quality_issues):
    """Calculate overall data quality score"""
    base_score = 100
    deductions = 0
    
    # Deduct points for quality issues
    for issue in quality_issues:
        severity = issue.get('severity', 'Medium')
        if severity == 'High':
            deductions += 20
        elif severity == 'Medium':
            deductions += 10
        else:
            deductions += 5
    
    # Deduct for high missing value percentages
    if 'columns' in profile_result:
        for col_info in profile_result['columns'].values():
            null_pct = col_info.get('null_percentage', 0)
            if null_pct > 50:
                deductions += 15
            elif null_pct > 20:
                deductions += 10
            elif null_pct > 10:
                deductions += 5
    
    final_score = max(0, base_score - deductions)
    
    # Determine category and color
    if final_score >= 90:
        category = "Excellent"
        color = "🟢"
    elif final_score >= 75:
        category = "Good"
        color = "🟡"
    elif final_score >= 60:
        category = "Fair"
        color = "🟠"
    else:
        category = "Poor"
        color = "🔴"
    
    return {
        'score': round(final_score, 1),
        'category': category,
        'color': color
    }

def generate_recommendations(profile_result, quality_issues):
    """Generate actionable recommendations"""
    recommendations = []
    
    # Recommendations based on quality issues
    high_issues = [issue for issue in quality_issues if issue.get('severity') == 'High']
    if high_issues:
        recommendations.append(
            f"🔴 **High Priority**: Address {len(high_issues)} critical data quality issues immediately."
        )
    
    # Recommendations for missing values
    if 'columns' in profile_result:
        high_missing_cols = [
            col for col, info in profile_result['columns'].items()
            if info.get('null_percentage', 0) > 20
        ]
        if high_missing_cols:
            recommendations.append(
                f"📊 **Missing Data**: Investigate high missing value rates in {len(high_missing_cols)} columns."
            )
    
    # General recommendations
    if not quality_issues:
        recommendations.append("✅ **Great Job**: Your data quality is excellent! Continue monitoring regularly.")
    else:
        recommendations.append("🔍 **Regular Monitoring**: Set up automated quality checks to catch issues early.")
    
    return recommendations

def show_statistical_outliers(table_name, column_name):
    """Show actual outlier records for a specific column"""
    try:
        from utils import run_query
        
        # First get the mean and standard deviation
        stats_query = f"""
        SELECT 
            AVG({column_name}) as mean_val,
            STDDEV({column_name}) as std_dev
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
        """
        
        stats_result = run_query(stats_query)
        if stats_result.empty:
            st.error("Could not calculate statistics for outlier detection.")
            return
            
        mean_val = stats_result.iloc[0]['mean_val']
        std_dev = stats_result.iloc[0]['std_dev']
        
        if std_dev == 0:
            st.warning("No variation in data - cannot detect statistical outliers.")
            return
        
        # Get outlier records (Z-score > 3)
        outliers_query = f"""
        SELECT *, 
               ABS({column_name} - {mean_val}) / {std_dev} as z_score
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
        AND ABS({column_name} - {mean_val}) / {std_dev} > 3
        ORDER BY z_score DESC
        LIMIT 50
        """
        
        outliers_df = run_query(outliers_query)
        
        if outliers_df.empty:
            st.info(f"No statistical outliers found for {column_name}")
            return
        
        st.subheader(f"🔍 Statistical Outliers in {column_name}")
        
        # Calculate analysis values first
        highest_outlier = outliers_df[column_name].max()
        lowest_outlier = outliers_df[column_name].min()
        highest_z_score = outliers_df['z_score'].max()
        lowest_z_score = outliers_df[outliers_df[column_name] == lowest_outlier]['z_score'].iloc[0]
        
        # Calculate what this means in business terms
        avg_value = mean_val
        normal_range_upper = avg_value + (2 * std_dev)  # 95% of normal values
        normal_range_lower = avg_value - (2 * std_dev)
        
        # Show user-friendly analysis results at the top
        st.success(f"""
        We found **{len(outliers_df)} unusual values** in your {column_name} data that stand out significantly from the typical range.
        
        ### 📈 **Context & Comparison:**
        - **Typical {column_name} range**: {normal_range_lower:,.2f} to {normal_range_upper:,.2f}
        - **Average {column_name}**: {avg_value:,.2f}
        - **Most extreme high value**: {highest_outlier:,.2f} *(that's {(highest_outlier/avg_value):,.1f}x higher than average!)*
        - **Most extreme low value**: {lowest_outlier:,.2f} *(that's {(avg_value/lowest_outlier):,.1f}x lower than average)*
        
        ### 🤔 **What This Means:**
        These outliers could indicate:
        - **Data entry errors** (someone typed an extra zero?)
        - **Special cases** (premium products, bulk discounts, etc.)
        - **System glitches** during data collection
        - **Genuine extreme values** that need investigation
        
        ### 💡 **Recommended Actions:**
        1. **Review the extreme values** in the table below
        2. **Check if they make business sense** in your context
        3. **Investigate data sources** for these specific records
        4. **Consider if corrections are needed** for obvious errors
        5. **Document any legitimate extreme values** for future reference
        """)
        
        # Show statistics metrics
        st.subheader("📋 Statistical Details")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Mean", f"{mean_val:.2f}")
        with col2:
            st.metric("Std Dev", f"{std_dev:.2f}")
        with col3:
            st.metric("Outliers Found", len(outliers_df))
        with col4:
            st.metric("Max Z-Score", f"{outliers_df['z_score'].max():.2f}")
        
        # Show the outlier records
        st.subheader("Detailed Outlier Records")
        st.write("**Records with Z-score > 3:**")
        
        # Format the dataframe for better display
        display_df = outliers_df.copy()
        display_df['z_score'] = display_df['z_score'].round(2)
        
        st.dataframe(
            display_df, 
            use_container_width=True,
            height=400
        )
        
    except Exception as e:
        st.error(f"Error showing outliers for {column_name}: {str(e)}")

if __name__ == "__main__":
    show_data_profiler()

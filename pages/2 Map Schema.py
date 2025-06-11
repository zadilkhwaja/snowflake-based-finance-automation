import streamlit as st
from config.snowflake_config import get_connection
from utils.save_mapping_to_snowflake import save_column_mappings
from utils.schema_utils import compare_schema

st.header("🧾 Step 2 — Compare with target schema")

table_choice = st.selectbox(
    "Choose target table",
    ["invoices", "payments", "purchase_orders"],
    key="table_selector"
)
st.session_state.table_name = table_choice

if st.session_state.uploaded_df is not None:
    session = get_connection()

    # Query the expected schema using Snowpark
    expected_df = (
        session.table("expected_schema")
        .filter(f"table_name = '{table_choice}'")
        .select("column_name")
    )

    expected_cols = [row["COLUMN_NAME"].lower() for row in expected_df.collect()]
    st.session_state.expected_columns = expected_cols

    comparison = compare_schema(st.session_state.actual_columns, expected_cols)
    st.session_state.missing_columns = comparison["missing"]

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Matching", len(comparison["matching"]))
    with col2:
        st.metric("Missing", len(comparison["missing"]))
    with col3:
        st.metric("Extra", len(comparison["extra"]))

    st.json(comparison, expanded=False)

    if comparison["missing"]:
        st.warning("We found missing columns. Map them below ⇣")

        with st.form("mapping_form"):
            for col in comparison["missing"]:
                selection = st.selectbox(
                    f"Map expected column **{col}** to →",
                    st.session_state.actual_columns,
                    key=f"map_{col}",
                )
                st.session_state.column_mapping[col] = selection

            st.session_state.vendor_id = st.text_input(
                "Vendor ID (for reusable mappings)", value="vendor_001"
            )

            submitted = st.form_submit_button("💾 Save mapping & continue")
            if submitted:
                save_column_mappings(
                    session,
                    st.session_state.vendor_id,
                    table_choice,
                    st.session_state.column_mapping,
                )
                st.success("Mapping stored in Snowflake 🎉")
    else:
        # Auto-create 1:1 mapping where expected = actual
        st.session_state.column_mapping = {
            col: col for col in comparison["matching"]
        }

        # Save vendor ID if not yet defined
        if "vendor_id" not in st.session_state:
            st.session_state.vendor_id = "vendor_001"  # or prompt earlier

        save_column_mappings(
            session,
            st.session_state.vendor_id,
            table_choice,
            st.session_state.column_mapping,
        )
        st.success("✅ All columns match! Mapping saved automatically 🎉")

if st.button("Convert Mapping"):
    st.switch_page("pages/3 Convert Mapping.py")
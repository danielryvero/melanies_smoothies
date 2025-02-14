# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched

# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Orders!:cup_with_straw:")
st.write(
    """Orders that need to be filled.
    """
)

#name_on_order = st.text_input("Pending Orders:")


session = get_active_session()
#my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
#st.dataframe(data=my_dataframe, use_container_width=True)

orders = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).to_pandas()
#st.write("Columns in orders:", orders.columns.tolist())

if orders.empty:
    st.write("No pending orders")
else:
    editable_df=st.data_editor(orders, num_rows="dynamic")
    if st.button("Submit"):
        updated_orders=editable_df[
            (editable_df["ORDER_FILLED"]==1)&(orders["ORDER_FILLED"]==0)
        ]
        if not updated_orders.empty:
            for order_name in updated_orders["NAME_ON_ORDER"]:
                update_query = f"""
                UPDATE smoothies.public.orders
                SET ORDER_FILLED = TRUE 
                WHERE NAME_ON_ORDER = '{order_name}'
                """
                session.sql(update_query).collect()
            st.success("Orders updated succesfully!", icon ="✅")
            #st.write (update_query)
        else:
            st.warning("No changes detected", icon="⚠️")
# try:
#     og_dataset = session.table("smoothies.public.orders")
#     edited_dataset = session.create_dataframe(orders)
#     og_dataset.merge(edited_dataset
#              , (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID'])
#              , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
#             )
#     st.success("Order(s) Updated!", icon = "👍")
# except:
#     st.write('Somwthing went wrong.')
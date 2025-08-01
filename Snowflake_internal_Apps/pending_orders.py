import streamlit as st
import time
from snowflake.snowpark.functions import col, when_matched
from snowflake.snowpark.context import get_active_session

#title to show
st.title(":cup_with_straw: Pending Smoothie Orders! :cup_with_straw:")
st.write("Orders that need to be filled.")

# get snowflake session
session = get_active_session()

# load pending orders
def load_pending_orders():
    return session.table("smoothies.public.orders").filter(col("ORDER_FILLED") == 0).to_pandas()

my_dataframe = load_pending_orders()

if not my_dataframe.empty:
    # show editable dataframe
    editable_df = st.data_editor(
        my_dataframe,
        hide_index=True,
        use_container_width=True,
        key="data_editor"
    )

    submitted = st.button('Submit')

    if submitted:
        # convert edited datagrane to Snowpark DataFrame
        edited_dataset = session.create_dataframe(editable_df)

        try:
            # get original dataset
            og_dataset = session.table("smoothies.public.orders")

            # merge to update ORDER_FILLED
            og_dataset.merge(
                edited_dataset,
                (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID']),
                [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
            )

            st.success("Order(s) Updated!", icon="👍")

            # rerun interface after sleep
            time.sleep(2)
            st.rerun()

        except Exception as e:
            st.error(f"Something went wrong: {str(e)}")
else:
    st.success('There are no pending orders right now', icon="👍")


## down below we have another version that works, but doesn't refresh the app when submitting    
# import streamlit as st
# from snowflake.snowpark.functions import col, when_matched
# from snowflake.snowpark.context import get_active_session

# # Write directly to the app
# st.title(":cup_with_straw: Pending Smoothie Orders!:cup_with_straw:")
# st.write(
#     """Orders that need to be filled.
#     """
# )
# # @st.fragment()
# # def repeat():
# #     session = get_active_session()
# #     my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0)
# #     # submitted = st.button('Submit')
# #     return editable_df

# session = get_active_session()
# my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0)#.to_pandas()

# if my_dataframe:
#     editable_df = st.data_editor(my_dataframe, hide_index=True, use_container_width=True)
#     submitted = st.button('Submit') 

    
#     if submitted:

#         og_dataset = session.table("smoothies.public.orders")
#         edited_dataset = session.create_dataframe(editable_df)
        
#         try:
#             og_dataset.merge(edited_dataset
#                          , (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID'])
#                          , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
#                         )
#             st.success("Order(s) Updated!", icon = "👍")
#             st.rerun()
#         except:
#             st.write('Something went wrong.')
# else:
#     st.success('There are no pending orders right now', icon = "👍")

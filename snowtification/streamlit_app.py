# Importing needed packages
from configparser import ConfigParser
import streamlit as st
import pandas as pd 
import snowflake.connector
import time
from datetime import datetime

# Reading the configuration file with 'configparser' package
config_sf = ConfigParser()
config_sf.sections()
config_sf.read('config_sf.ini')

# Assigning snowflake configuration (Environment)
sfAccount = config_sf['Snowflake']['sfAccount']
sfUser = config_sf['Snowflake']['sfUser']
sfPassword = config_sf['Snowflake']['sfPassword']
sfWarehouse = config_sf['Snowflake']['sfWarehouse']
sfDatabase = config_sf['Snowflake']['sfDatabase']
sfSchema = config_sf['Snowflake']['sfSchema']
sfRole = config_sf['Snowflake']['sfRole']

# Connect to Snowflake using the configurations above
conn = snowflake.connector.connect(
    user=sfUser,
    password=sfPassword,
    account=sfAccount,
    warehouse=sfWarehouse,
    database=sfDatabase,
    schema=sfSchema,
    role=sfRole
    )

# Open connection
c = conn.cursor()

def current_time():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

#################################################################################################
# ALERTS
#################################################################################################
def get_alerts():
    c.execute("SELECT * FROM alert")
    data = c.fetchall()
    # for row in data:
    #   print(row)
    return data

def get_warehouses():
    c.execute("SHOW WAREHOUSES")
    data = c.fetchall()
    # for row in data:
    #   print(row)
    return data    

def get_databases():
    c.execute("SHOW DATABASES")
    data = c.fetchall()
    # for row in data:
    #   print(row)
    return data    

def get_db_schemas(db_nm):
    c.execute("SHOW SCHEMAS in DATABASE {}".format(db_nm))
    data = c.fetchall()
    return data    

def add_alert(database_nm, schema_nm, alert_nm, warehouse_nm, schedule_cd, action_type_cd, comment_tx, custom_condition_tx, custom_action_tx):
    c.execute('INSERT INTO alert(database_nm, schema_nm, alert_nm, warehouse_nm, schedule_cd, action_type_cd, 0, comment_tx, insert_ts, update_ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);',(database_nm, schema_nm, alert_nm, warehouse_nm, schedule_cd, action_type_cd, comment_tx, current_time(), current_time()))
    
    c.execute('INSERT INTO condition(alert_id, custom_condition_tx, insert_ts, update_ts) VALUES (%s, %s, %s, %s);',(alert_id, custom_condition_tx, current_time(), current_time()))
    c.execute('INSERT INTO action(alert_id, custom_action_tx, insert_ts, update_ts) VALUES (%s, %s, %s, %s);',(alert_id, custom_action_tx, current_time(), current_time()))
    conn.commit()

def update_alert(alert_id, database_nm, schema_nm, alert_nm, warehouse_nm, schedule_cd, action_type_cd, alert_suspend_ind, comment_tx):
    c.execute('UPDATE alert SET alert_nm=%s, database_nm=%s, schema_nm=%s, warehouse_nm=%s, schedule_cd=%s, action_type_cd=%s, alert_suspend_ind=%s, comment_tx=%s, update_ts=%s WHERE alert_id=%s',(alert_nm, database_nm, schema_nm, warehouse_nm, schedule_cd, action_type_cd, alert_suspend_ind, comment_tx, current_time()), alert_id)
    conn.commit() 

def delete_alert(alert_id):
    c.execute("DELETE FROM alert WHERE alert_id='{}'".format(alert_id))
    conn.commit()

def get_single_alert(alert_nm):
    c.execute("SELECT * FROM alert WHERE alert_nm='{}'".format(alert_nm))
    data = c.fetchall()
    return data

#################################################################################################
# ATTRIBUTES
#################################################################################################
def get_attributes():
    c.execute("SELECT * FROM attribute")
    data = c.fetchall()
    # for row in data:
    #   print(row)
    return data

def add_attribute(attribute_nm):
    c.execute('INSERT INTO attribute(attribute_nm,insert_ts,update_ts) VALUES (%s, %s, %s);',(attribute_nm,current_time(),current_time()))
    conn.commit()

def update_attribute(attrib_id,attribute_nm_input):
    c.execute('UPDATE attribute SET attribute_nm=%s,update_ts=%s WHERE attribute_id=%s',(attribute_nm_input,current_time(),attrib_id))
    conn.commit() 

def delete_attribute(attribute_nm):
    c.execute("DELETE FROM attribute WHERE attribute_nm='{}'".format(attribute_nm))
    conn.commit()

def get_single_attribute(attribute_nm):
    c.execute("SELECT * FROM attribute WHERE attribute_nm='{}'".format(attribute_nm))
    data = c.fetchall()
    return data
#################################################################################################
# TEMPLATES
#################################################################################################
def get_templates():
    c.execute("SELECT * FROM action_template")
    data = c.fetchall()
    # for row in data:
    #   print(row)
    return data

def add_template(action_template_nm, action_template_cd, action_template):
    c.execute('INSERT INTO action_template(action_template_nm, action_template_cd, action_template, insert_ts, update_ts) VALUES (%s, %s, %s, %s, %s);',(action_template_nm, action_template_cd, action_template, current_time(), current_time()))
    conn.commit()

def update_template(action_template_id, action_template_nm, action_template_cd, action_template):
    c.execute('UPDATE action_template SET action_template_nm=%s, action_template_cd=%s, action_template=%s, update_ts=%s WHERE action_template_id=%s',(action_template_nm, action_template_cd, action_template, current_time()), action_template_id)
    conn.commit() 

def delete_template(action_template_nm):
    c.execute("DELETE FROM action_template WHERE action_template_nm='{}'".format(action_template_nm))
    conn.commit()

def get_single_template(action_template_nm):
    c.execute("SELECT * FROM action_template WHERE action_template_nm='{}'".format(action_template_nm))
    data = c.fetchall()
    return data


def main():
    """Snowtification"""
    html_temp = """
        <div style="background-color:{};padding:10px;border-radius:10px">
        <h1 style="color:{};text-align:center;">Snowtification</h1>
        </div>
        """
    st.markdown(html_temp.format('royalblue','white'),unsafe_allow_html=True)
        
    title_temp ="""

        <div style="background-color:#464e5f;padding:10px;border-radius:10px;margin:10px;">
        <h4 style="color:white;text-align:center;">{}</h1>
        <img src="https://www.w3schools.com/howto/img_avatar.png" alt="Avatar" style="vertical-align: middle;float:left;width: 50px;height: 50px;border-radius: 50%;" >
        <h6>Author:{}</h6>
        <br/>
        <br/>   
        <p style="text-align:justify">{}</p>
        </div>
        """

    article_temp ="""
        <div style="background-color:#464e5f;padding:10px;border-radius:5px;margin:10px;">
        <h4 style="color:white;text-align:center;">{}</h1>
        <h6>Author:{}</h6> 
        <h6>Post Date: {}</h6>
        <img src="https://www.w3schools.com/howto/img_avatar.png" alt="Avatar" style="vertical-align: middle;width: 50px;height: 50px;border-radius: 50%;" >
        <br/>
        <br/>
        <p style="text-align:justify">{}</p>
        </div>
        """

    full_message_temp ="""
        <div style="background-color:silver;padding:10px;border-radius:5px;margin:10px;">
            <p style="text-align:justify;color:black;padding:10px">{}</p>
        </div>
        """
#################################################################################################
# ALERTS
#################################################################################################
    single_alert_view ="""
        <div style="background-color:#464e5f;padding:10px;border-radius:5px;margin:2px;">
        <h6>ID: <font color=black>{}</font></h6>     
        <h6>Database Name: <font color=black>{}</font></h6>    
        <h6>Schema Name: <font color=black>{}</font></h6> 
        <h6>Alert Name: <font color=black>{}</font></h6> 
        <h6>Warehouse Name: <font color=black>{}</font></h6> 
        <h6>Schedule Name: <font color=black>{}</font></h6> 
        <h6>Action: <font color=black>{}</font></h6> 
        <h6>Alert Suspended: <font color=black>{}</font></h6> 
        <h6>Comments: <font color=black>{}</font></h6>   
        <h6>Insert Timestamp: <font color=black>{}</font></h6>
        <h6>Update Timestamp: <font color=black>{}</font></h6>
        
        </div>
        """

#################################################################################################
# ATTRIBUTES
#################################################################################################
    single_attribute_view ="""
        <div style="background-color:#464e5f;padding:10px;border-radius:5px;margin:2px;">
        <h6>ID: <font color=black>{}</font></h6>     
        <h6>Attribute Name: <font color=black>{}</font></h6>      
        <h6>Insert Timestamp: <font color=black>{}</font></h6>
        <h6>Update Timestamp: <font color=black>{}</font></h6>
        
        </div>
        """
#################################################################################################
# TEMPLATES
#################################################################################################
    single_template_view ="""
        <div style="background-color:#464e5f;padding:10px;border-radius:5px;margin:2px;">
        <h6>ID: <font color=black>{}</font></h6>     
        <h6>Template Code: <font color=black>{}</font></h6> 
        <h6>Template Name: <font color=black>{}</font></h6> 
        <h6>Template: <font color=black>{}</font></h6>      
        <h6>Insert Timestamp: <font color=black>{}</font></h6>
        <h6>Update Timestamp: <font color=black>{}</font></h6>
        
        </div>
        """



    menu = ["Alerts", "Integrations", "Templates", "Attributes", "Template Attributes"]

    choice = st.sidebar.selectbox("Menu",menu)

    if choice == "Home":
        st.subheader("Home")        
        #result = view_all_notes()
        for i in result:
            # short_article = str(i[2])[0:int(len(i[2])/2)]
            short_article = str(i[2])[0:50]
            st.write(title_temp.format(i[1],i[0],short_article),unsafe_allow_html=True)
        # st.write(result)

#################################################################################################
# ALERTS
#################################################################################################

    elif choice == "Alerts": 

        def update():
            st.session_state.text += st.session_state.text_value1
            st.session_state.text += st.session_state.text_value2

        ## ADD NEW TEMPLATE
        with st.form(key='new_alert_form', clear_on_submit=True):
            st.subheader("Add New Alert")

            all_databases = [i[1] for i in get_databases()]
            dblist = st.selectbox("Create Alert on Database",all_databases)

            schema_list = get_db_schemas(dblist)
            schemanm = [i[1] for i in schema_list]
            schema_nm = st.selectbox("Create Alert on Schema",options=schemanm) 

            action_alert_nm = st.text_input('Enter Alert Name')

            all_warehouses = [i[0] for i in get_warehouses()]
            wh_list = st.selectbox("Use Warehouse for this Alert",all_warehouses)

            action_alert_schedule = st.text_input('Alert Schedule: (e.g. <num> MINUTE | USING CRON <expr> <time zone>)')
            action_alert_type = st.selectbox('Alert Type',('Query', 'Email'))
            action_alert_condition = st.text_area('Condition (SQL, JINJA, Integration)', height=200)
            action_alert_action = st.text_area('Action (SQL, JINJA, Integration)', height=200)

            if st.form_submit_button("Add Alert"):
                add_template(dblist, schema_list, action_alert_nm, wh_list, action_alert_schedule, action_alert_type, action_alert_condition, action_alert_action)
                st.success("Alert: '{}' Saved".format(action_alert_nm))
                st.experimental_rerun()

        ## SELECT ALL ALERTS
        st.subheader("View Alerts")
        all_alerts = [i[3] for i in get_alerts()]
        postlist = st.selectbox("Alerts",all_alerts)

        ## SELECT SIGNLE ALERT
        post_result = get_single_alert(postlist)
        for i in post_result:
            suspend_ind = i[7]
            st.markdown(single_alert_view.format(i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7],i[8],i[9],i[10]),unsafe_allow_html=True)  

        ## DELETE SELECTED ALERT
        if st.button("Delete"):
            delete_alert(i[0])
            st.warning("Alert: '{}' Deleted".format(i[0])) 
            st.experimental_rerun()

        ## UPDATE SELECTED ALERT
        with st.form(key='my_form', clear_on_submit=True):
            st.subheader("Update Selected Alert")
            alert_nm = st.text_input('Alert Name', value=i[3], key='text_value1')
            schedule_cd = st.text_input('Schedule', value=i[5], key='text_value2')
            values = ['QUERY', 'EMAIL']
            default_ix = values.index(i[6])   
            action_alert_type = st.selectbox('Alert Type',('Query', 'Email'),index=default_ix)
            if action_alert_type == 'Query':
                action_alert_type = 'QUERY'
            elif action_alert_type == 'Email':
                action_alert_type = 'EMAIL'            
            action_alert_condition = st.text_area('Condition (SQL, JINJA, Integration)', height=200, value='TEST')   
            action_alert_action = st.text_area('Action (SQL, JINJA, Integration)', height=200, value='TEST')  

            if suspend_ind == 0:
                alert_suspend_ind = st.checkbox('Suspend Alert', value=False) 
            elif suspend_ind == 1:
                alert_suspend_ind = st.checkbox('Suspend Alert', value=True)     

            attribute_update_btn = st.form_submit_button(label='Update', on_click=update)
            if 'text' not in st.session_state:
                st.session_state.text = ""                   
            if attribute_update_btn and attribute_nm_input:
                update_alert(alert_nm, schedule_cd, action_alert_type, alert_suspend_ind, action_alert_condition, action_alert_action)
                st.success("Updated")  
                st.experimental_rerun()

#################################################################################################
# ATTRIBUTES
#################################################################################################

    elif choice == "Attributes": 

        def update():
            st.session_state.text += st.session_state.text_value

        ## ADD NEW ATTRIBUTES
        with st.form(key='new_attr_form', clear_on_submit=True):
            st.subheader("Add Your Attribute")
            attribute_nm = st.text_input('Enter Attribute Name')
            if st.form_submit_button("Add"):
                add_attribute(attribute_nm)
                st.success("Attribute: '{}' Saved".format(attribute_nm))
                st.experimental_rerun()

        ## SELECT ALL ATTRIBUTES
        st.subheader("View Attributes")
        all_attributes = [i[1] for i in get_attributes()]
        postlist = st.selectbox("Attributes",all_attributes)

        ## SELECT SIGNLE ATTRIBUTE
        post_result = get_single_attribute(postlist)
        for i in post_result:
            attrib_id = i[0]
            st.markdown(single_attribute_view.format(i[0],i[1],i[2],i[3]),unsafe_allow_html=True)   

        ## DELETE SELECTED ATTRIBUTE
        if st.button("Delete"):
            delete_attribute(i[1])
            st.warning("Attribute: '{}' Deleted".format(i[1])) 
            st.experimental_rerun()

        ## UPDATE SELECTED ATTRIBUTE
        with st.form(key='my_form', clear_on_submit=True):
            st.subheader("Update Selected Attribute")
            attribute_nm_input = st.text_input('Attribute Name', value="", key='text_value')
            attribute_update_btn = st.form_submit_button(label='Update', on_click=update)
            if 'text' not in st.session_state:
                st.session_state.text = ""                   
            if attribute_update_btn and attribute_nm_input:
                update_attribute(attrib_id,attribute_nm_input)
                st.success("Updated")  
                st.experimental_rerun()

#################################################################################################
# TEMPLATES
#################################################################################################

    elif choice == "Templates": 

        def update():
            st.session_state.text += st.session_state.text_value

        ## ADD NEW TEMPLATE
        with st.form(key='new_temp_form', clear_on_submit=True):
            st.subheader("Add New Template")
            action_template_nm = st.text_input('Enter Template Name')
            action_template_cd = st.selectbox('Template Code',('Query', 'Integration'))
            action_template = st.text_area('Template (SQL, JINJA, Integration)', height=200)
            if st.form_submit_button("Add"):
                if action_template_cd == 'Query':
                    action_template_cd = 'QUERY'
                elif action_template_cd == 'Integration':
                    action_template_cd = 'INTEG'
                add_template(action_template_nm, action_template_cd, action_template)
                st.success("Template: '{}' Saved".format(action_template_nm))
                st.experimental_rerun()

        ## SELECT ALL TEMPLATES
        st.subheader("View Templates")
        all_templates = [i[2] for i in get_templates()]
        postlist = st.selectbox("Templates",all_templates)

        ## SELECT SIGNLE TEMPLATE
        post_result = get_single_template(postlist)
        for i in post_result:
            action_template_id = i[0]
            st.markdown(single_template_view.format(i[0],i[1],i[2],i[3],i[4],i[5]),unsafe_allow_html=True)   

        ## DELETE SELECTED TEMPLATE
        if st.button("Delete"):
            delete_template(i[2])
            st.warning("Template: '{}' Deleted".format(i[1])) 
            st.experimental_rerun()

        ## UPDATE SELECTED TEMPLATE
        with st.form(key='my_form', clear_on_submit=True):
            st.subheader("Update Selected Template")
            action_template_nm = st.text_input('Template Name', value=i[2], key='text_value')
            values = ['QUERY', 'INTEG']
            default_ix = values.index(i[1])   
            action_template_cd = st.selectbox('Template Code',('Query', 'Integration'),index=default_ix)
            if action_template_cd == 'Query':
                action_template_cd = 'QUERY'
            elif action_template_cd == 'Integration':
                action_template_cd = 'INTEG'            
            action_template = st.text_area('Template (SQL, JINJA, Integration)', height=200, value=i[3])            
            template_update_btn = st.form_submit_button(label='Update', on_click=update)
            if 'text' not in st.session_state:
                st.session_state.text = ""                   
            if template_update_btn and action_template:
                update_template(action_template_id, action_template_nm, action_template_cd, action_template)
                st.success("Updated")  
                st.experimental_rerun()

if __name__ == '__main__':
    main()
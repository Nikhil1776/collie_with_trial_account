from flask import Flask, request, render_template, jsonify
from flask_cors import CORS, cross_origin
import snowflake.connector
import json

app = Flask(__name__, static_folder="build/static", template_folder="build") # to host react frontend static files
cors = CORS(app)
app.config["CORS_HEADERS"] = "Content-Type"

@app.errorhandler(404)
def resource_not_found(error):
    return jsonify(message=str(error))

@app.errorhandler(500)
def server_error(error):
    return jsonify(message=str(error))

@app.route("/")
@cross_origin()
def frontend():
    return render_template('index.html')

@app.route("/health")
@cross_origin()
def health():
    return jsonify(message="API health is good")
    
@app.route("/test")
@cross_origin()
def test():
    return jsonify(message="Test endpoint request received")

@app.route('/login', methods=['POST'])
def login():
    body = request.get_json()
    userName = body['userName']
    account = body['account']
    password = body['password']
    try:
        connection = snowflake.connector.connect(
            user=userName,
            password=password,
            account=account,
            warehouse='COMPUTE_WH',
            database='ACCELERATOR_DB',
            schema='RBAC',
            role='SYSADMIN'
        )
        if connection:
            return {'success': True, 'userCreds': {'username': userName, 'password': password, 'account': account}}
        else:
            return {'success': False}
    except snowflake.connector.errors.DatabaseError:
        return {'success': False}

@app.route("/create_role/", methods=["POST"])
@cross_origin()
def create_role():
    print("creating connection")
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()
        # return one_row[0]
        cs = ctx.cursor()
        print("conenction created")
        data = data_["data"]
        print(data)
        create_role_query = f"call SP_CREATE_ROLE('{data[0]}','{data[1]}','{data[2]}','{data[3]}');"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}

@app.route("/drop_role/", methods=["POST"])
@cross_origin()
def drop_role():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()

        # return one_row[0]
        cs = ctx.cursor()
        data = data_["data"]
        print(data)
        create_role_query = f"call SP_DROP_ROLE('{data[0]}','{data[1]}','{data[2]}');"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}

@app.route("/create_user/", methods=["POST"])
@cross_origin()
def create_user():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()
        # return one_row[0]
        cs = ctx.cursor()
        data = data_["data"]
        print(data)
        create_role_query = f"call SP_CREATE_USER('{data[0]}','{data[1]}','{data[2]}');"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}

@app.route("/drop_user/", methods=["POST"])
@cross_origin()
def drop_user():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()
        # return one_row[0]
        cs = ctx.cursor()
        data = data_["data"]
        print(data)
        create_role_query = f"call SP_DROP_USER('{data[0]}',array_construct({str(data[1])[1:-1]}));"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}

@app.route("/create_warehouse/", methods=["POST"])
@cross_origin()
def create_warehouse():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()
        # return one_row[0]
        cs = ctx.cursor()
        data = data_["data"]
        print(data)
        create_role_query = f"call SP_CREATE_WAREHOUSE('{data[0]}','{data[1]}','{data[2]}',{data[3]},{data[4]},'{data[5]}',{data[6]},'{data[7]}','{data[8]}');"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}

@app.route("/grant_privs_to_role/", methods=["POST"])
@cross_origin()
def grant_privs_to_role():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()
        # return one_row[0]
        cs = ctx.cursor()
        data = data_["data"]
        print(data)
        create_role_query = f"call SP_GRANT_PRIVS_TO_ROLE('{data[0]}','{data[1]}','{data[2]}',array_construct({str(data[3])[1:-1]}),'{data[4]}','{data[5]}',array_construct({str(data[6])[1:-1]}));"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}
    
@app.route("/revoke_privs_from_role/", methods=["POST"])
@cross_origin()
def revoke_privs_from_role():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()

        # return one_row[0]
        cs = ctx.cursor()
        data = data_["data"]
        print(data)
        create_role_query = f"call SP_REVOKE_PRIVS_FROM_ROLE('{data[0]}','{data[1]}','{data[2]}',array_construct({str(data[3])[1:-1]}),'{data[4]}','{data[5]}',array_construct({str(data[6])[1:-1]}));"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}


@app.route("/grant_role_to_role/", methods=["POST"])
@cross_origin()
def grant_role_to_role():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()

        # return one_row[0]
        cs = ctx.cursor()
        data = data_["data"]
        print(data)
        create_role_query = (
            f"call SP_GRANT_ROLE_TO_ROLE('{data[0]}','{data[1]}','{data[2]}');"
        )
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}
    
    
@app.route("/grant_role_to_user/", methods=["POST"])
@cross_origin()
def grant_role_to_user():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()

        # return one_row[0]
        cs = ctx.cursor()
        data = data_["data"]
        #str_1 = str(data)
        #data_ = str_1[1:-1]
        print(data)
        create_role_query = f"call SP_GRANT_ROLE_TO_USER('{data[0]}',array_construct({str(data[1])[1:-1]}),array_construct({str(data[2])[1:-1]}))"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}

@app.route("/revoke_role/", methods=["POST"])
@cross_origin()
def revoke_role():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()
        # return one_row[0]
        cs = ctx.cursor()
        data = data_["data"]
        print(data)
        create_role_query = (
            f"call SP_REVOKE_ROLE('{data[0]}',array_construct({str(data[1])[1:-1]}),'{data[2]}',array_construct({str(data[3])[1:-1]}));"
        )
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}

@app.route("/drop_warehouse/", methods=["POST"])
@cross_origin()
def drop_warehouse():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()
        # return one_row[0]
        cs = ctx.cursor()
        data = data_["data"]
        print(data)
        create_role_query = f"call SP_DROP_WAREHOUSE('{data[0]}',array_construct({str(data[1])[1:-1]}));"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}

@app.route("/return_users_of_role/", methods=["POST"])
@cross_origin()
def return_users_of_role():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()
        # return one_row[0]
        cs = ctx.cursor()
        data_ = request.get_json()
        data = data_["data"]
        print(data)
        create_role_query = f"call SP_RETURN_USERS_OF_ROLE('{data[0]}');"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}
    
@app.route("/replicate_priv_to_user/", methods=["POST"])
@cross_origin()
def replicate_priv_to_user():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()

        # return one_row[0]
        cs = ctx.cursor()
        data_ = request.get_json()
        data = data_["data"]
        #str_1 = str(data)
        #data_ = str_1[1:-1]
        print(data)
        create_role_query = f"call SP_REPLICATE_PRIV_TO_USER('{data[0]}',array_construct({str(data[1])[1:-1]}),'{data[2]}')"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}


@app.route("/return_roles/", methods=["POST"])
@cross_origin()
def return_roles():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()
        # return one_row[0]
        cs = ctx.cursor()
        create_role_query = f"call SP_RETURN_ROLES()"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}

@app.route("/return_users/", methods=["POST"])
@cross_origin()
def return_users():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()
        # return one_row[0]
        cs = ctx.cursor()
        create_role_query = f"call SP_RETURN_USERS()"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}

@app.route("/rbac_hirerchy/", methods=["POST"])
@cross_origin()
def rbac_hirerchy():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()
        # return one_row[0]
        cs = ctx.cursor()
        create_role_query = "call SP_RBAC_HIERARCHY();"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        print(res)
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}

@app.route("/independent_roles/", methods=["POST"])
@cross_origin()
def independent_roles():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()
        # return one_row[0]
        cs = ctx.cursor()
        create_role_query = "call SP_INDEPENDENT_ROLES();"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}
    
@app.route("/return_warehouse/", methods=["POST"])
@cross_origin()
def return_warehouse():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()
        # return one_row[0]
        cs = ctx.cursor()
        create_role_query = "call sp_return_warehouses();"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}

@app.route("/return_resource_monitors/", methods=["POST"])
@cross_origin()
def return_resource_monitors():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()

        # return one_row[0]
        cs = ctx.cursor()
        create_role_query = "call sp_return_resource_monitors();"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}

@app.route("/return_integrations/", methods=["POST"])
@cross_origin()
def return_integrations():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()

        # return one_row[0]
        cs = ctx.cursor()
        create_role_query = "call sp_return_integrations();"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}


@app.route("/return_databases/", methods=["POST"])
@cross_origin()
def return_databases():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()

        # return one_row[0]
        cs = ctx.cursor()
        create_role_query = "call sp_return_databases();"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}

@app.route("/return_schemas/", methods=["POST"])
@cross_origin()
def return_schemas():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()
        # return one_row[0]
        cs = ctx.cursor()
        data_ = request.get_json()
        data = data_["data"]
        print(data)
        create_role_query = f"call SP_RETURN_SCHEMAS('{data[0]}');"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}

@app.route("/return_schema_object_lists/", methods=["POST"])
@cross_origin()
def return_schema_object_lists():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
        user=userName,
        password=password,
        account=account,
        database="ACCELERATOR_DB",
        schema="RBAC",
        role="accountadmin",
        warehouse="COMPUTE_WH",
        )
        ctx.cursor()
        # return one_row[0]
        cs = ctx.cursor()
        data_ = request.get_json()
        data = data_["data"]
        print(data)
        create_role_query = f"call SP_RETURN_OBJECT_LIST('{data[0]}', '{data[1]}', '{data[2]}');"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}
    
@app.route("/return_tag_values/", methods=["POST"])
@cross_origin()
def return_tag_values():
    data_ = request.get_json()
    userName = data_["userName"]
    password = data_["password"]
    account = data_["account"]
    try:
        ctx = snowflake.connector.connect(
            user=userName,
            password=password,
            account=account,
            database="ACCELERATOR_DB",
            schema="RBAC",
            role="accountadmin",
            warehouse="COMPUTE_WH",
        )
        ctx.cursor()

        # return one_row[0]
        cs = ctx.cursor()
        create_role_query = "call SP_RETURN_TAG_VALUES();"
        print(create_role_query)
        cs.execute(create_role_query)
        res = cs.fetchone()
        print(res)
        res_ = json.loads(res[0])
        print(res_)
        # print(type(jsonify(res[0])))
        return jsonify(res_)
    except:
        return {'success': False}


if __name__ == '__main__':
	app.run(debug=True)

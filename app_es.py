from flask import Flask, render_template, request, flash, session, url_for, redirect
from wtforms import StringField, SubmitField, PasswordField, SelectField, validators
import flask_wtf #import FlaskFormimport
from wtforms.fields.html5 import DateField
from elasticsearch import Elasticsearch
import json

class LoginForm(flask_wtf.FlaskForm):
    username = StringField('User name')
    password = PasswordField('Password')
    connect = SubmitField('Connect to Elasticsearch')
    #register = SubmitField('Create New Account')
    #entrydate =DateField()

class BookingForm(flask_wtf.FlaskForm):
    es_index = SelectField('Existing Indices',choices=[])
    documents = SelectField('Document',choices=[])
    create = SubmitField('Create an index')
    go = SubmitField('Retrieve Index')
    new_index = StringField('New Index')#),validators=[validators.DataRequired()])
    delete=  SubmitField('Delete selected Index')
    add_document = SubmitField('Add a new document')
    delete_document = SubmitField('Delete a document')
    new_document = StringField('New Document')#),validators=[validators.DataRequired()])






    entrydate =DateField('Date')#,validators=[validators.DataRequired()])
    timeslot = SelectField('Time Slot',choices=['9:30am','10:30am','11:30am','13:30pm','14:30pm','15:30pm'])
    
    update=  SubmitField('Update selected Appointment')
    appointments =SelectField('Appointments',choices=[])
    back=  SubmitField('Back to Manual')

class UpdateForm(flask_wtf.FlaskForm):
    firstname = StringField('First Name',validators=[validators.DataRequired()])
    lastname = StringField('Last Name',validators=[validators.DataRequired()])
    email = StringField('email',validators=[validators.Email(granular_message=True)])
    telephone = StringField('Telephone',validators=[validators.DataRequired()])
    gender = SelectField('Gender',choices=['M','F'])
    age = StringField('Age',validators=[validators.DataRequired()])
    update = SubmitField('Update your Account')
    delete = SubmitField('Delete your Account')
    booking = SubmitField('Create an Appointment')

#####################################functions===================
def create_index(es, index):
    body = dict()
    #body['settings'] = get_setting()
    body['mappings'] = get_mappings()
    print(json.dumps(body)) #examine the format    
    res = es.indices.create(index=index , body=body,ignore = 400)
    #print(res)


def get_setting():
    settings = {
        "index": {
            "number_of_shards": 3,
            "number_of_replicas": 2
        }
    }
    return settings


def get_mappings():
    mappings = {
        "properties": {
            "location_on_disk": {"type": "text"}  
                      }
               } #mappings
    return mappings

def load_data(locations):
    data = list()
    #indces = ["6cafe024c7e9f79dcb654fdc34b2577a","cb404b5abaff0e2c302790c3d698d53a","dc186f2d44cf7389606ed1da176aa854"]
    #locations =["/var/www/data/example.txt", "/var/www/data/downloads/test.txt", "/var/www/data/huge_file.mp4"]
    for location in locations:
        data.append(
            {
                "location_on_disk": location
            }
        )
    return data

def create_data(es, index, data):
    for d in data:
        res =es.index(index=index, body=d) 
        print(res['result'])

def get_query(field, val):
    query = {
        "query": {
            "bool": {
                "must": [
                 {
                    "match_phrase": {
                        field: val
                                    }}]
            }}}
    return query


app = Flask(__name__)
app.config['SECRET_KEY'] = 'YOUR_SECRET_KEY'

@app.route('/', methods=['GET', 'POST'])
@app.route('/home', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    username = form.username.data
    password = form.password.data
    if not(username is None or password is None) and form.connect.data: 
        es = Elasticsearch(hosts=['localhost:9200'], http_auth=(username, password))
        if es.ping():
            return  redirect(url_for('booking',password=password, username=username ))
        else:
            return render_template('login.html', form = form, message = "incorrect passsword or user name" )
    else:
        return render_template('login.html', form = form, message = '' )

@app.route('/booking/<password>/<username>', methods=['GET', 'POST'])
def booking(password, username):
    form = BookingForm()
    es = Elasticsearch(hosts=['localhost:9200'], http_auth=(username, password))
    index_list = list()
    form.es_index.choices = []
    for index in es.indices.get('*'):
        if index == '.security-7':
            continue
        index_list.append(index)
    
    form.es_index.choices = index_list

    if form.go.data:
        query ={"query": {"match_all": {}}}
        selected_index = form.es_index.data
        result = es.search(index=selected_index, body=query)
        num_of_docs = len(result['hits']['hits'])
        if num_of_docs > 0:
            index_message = selected_index + ' has ' + str(num_of_docs)+ ' documents.'
            doc_list = []
            for hit in result['hits']['hits']:
                doc_list.append(hit['_source']['location_on_disk'])
            form.documents.choices = doc_list
            
        else:
            index_message = 'Selected index '+ selected_index + ' has no ' + ' documents.'
        return render_template('booking.html', form = form, message_index=index_message ) 

    if form.create.data:
        new_index = form.new_index.data
        if new_index is None or len(new_index)==0:
            return render_template('booking.html', form = form, message_create_index='Please input index')
        else:
            is_exist = es.indices.exists(index=new_index)
            if is_exist:
                return render_template('booking.html', form = form, \
                    message_create_index='Index has already existed. No index is created')
            else:
                create_index(es, new_index)
                is_created = es.indices.exists(index=new_index)
                index_list = []
                if is_created:
                    form.es_index.choices = []
                    for index in es.indices.get('*'):
                        if index == '.security-7':
                            continue
                        index_list.append(index)
                    form.es_index.choices = index_list
                    return render_template('booking.html', form = form, \
                         message_create_index='New Index ' + new_index + ' has been created')

    if form.delete.data:
        deleted_index = form.es_index.data
        is_exist = es.indices.exists(index=deleted_index)
        if is_exist: 
            es.indices.delete(index=deleted_index, ignore=[400, 404])
            is_deleted = not es.indices.exists(index=deleted_index)
            if is_deleted:
                index_list = []
                form.es_index.choices = []
                for index in es.indices.get('*'):
                    if index == '.security-7':
                        continue
                    index_list.append(index)
                    form.es_index.choices = index_list
                return render_template('booking.html', form = form, \
                    message_index='Selected Index ' + deleted_index + ' has been deleted')
    
    if form.add_document.data:
        new_document = form.new_document.data
        if new_document is None or len(new_document)==0:
            return render_template('booking.html', form = form, message_document='Please input document data')
        else:
            index_to_add_document = form.es_index.data
            data = load_data([new_document])
            create_data(es, index_to_add_document,data)
            doc_list = form.documents.choices
            query ={"query": {"match_all": {}}}
            result = es.search(index=index_to_add_document, body=query)
            
            for hit in result['hits']['hits']:
                doc_list.append(hit['_source']['location_on_disk'])
            doc_list.append(new_document)
            form.documents.choices = doc_list
            return render_template('booking.html', form = form, message_document='New document has been added')
    
    if form.delete_document.data:
        doc_list = []
        index_to_delete_document = form.es_index.data
        document_to_be_deleted = form.documents.data
        query ={"query": {"match_all": {}}}
        result = es.search(index=index_to_delete_document, body=query)            
        for hit in result['hits']['hits']:
            doc_list.append(hit['_source']['location_on_disk'])       

        
        query = get_query("location_on_disk", document_to_be_deleted )
        result = es.search(index=index_to_delete_document, body=query)
        result = result['hits']['hits']
        for hit in result:
            id = hit['_id']
            es.delete(index=index_to_delete_document,id=id)
            doc_list.remove(document_to_be_deleted)
        form.documents.choices = doc_list      
        return render_template('booking.html', form = form, message_document='document deleted')






        
    return render_template('booking.html', form = form, message='', msg='**error: No appointment is selected.') 



if __name__ == '__main__':
     app.run(host='0.0.0.0') 
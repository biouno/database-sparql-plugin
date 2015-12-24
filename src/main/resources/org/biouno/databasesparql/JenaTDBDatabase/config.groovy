package org.biouno.databasesparql.JenaTDBDatabase;

def f = namespace(lib.FormTagLib)

f.entry(field:"location", title:_("Location")) {
    f.textbox()
}
f.advanced {
    f.entry(field:"mustExist", title:_("Must Exist")) {
    f.textbox()
}
}
f.block {
    f.validateButton(method:"validate",title:_("Test Connection"),with:"location,mustExist")
}
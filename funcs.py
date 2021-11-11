import io
import os

# se llama el win32 dentro de la funcion puesto que puede que esta funcion se ejecute en otro OS que no sea windows.
def email(mensaje,basePath, error=None, adjunto=None):
    try:
        import win32com.client as win32
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        print_("Inicia lectura de JSON config: ") 
        params_fn = os.path.join(basePath, "config.json")
        params_io = io.open(params_fn)
        params_str = params_io.read()
        params_io.close()
        params = json.loads(params_str , object_pairs_hook=OrderedDict)
        print("Finalizacion lectura de JSON config cerrando...")
        CodeOwnerMail = params['CodeOwnerMail']
        CodeUserMail = params['CodeUserMail']
        #Email Personalizado, los detinatarios se deben separa por ;
        mail.To = CodeOwnerMail + ";" + CodeUserMail
        if error == 1:
            mail.Subject = '📰 Error, leer contenido!'
        else:
            mail.Subject = '📰 Proceso de carga ha finalizado!'
        
        Format = { 'UNSPECIFIED' : 0, 'PLAIN' : 1, 'HTML' : 2, 'RTF'  : 3}
        mail.BodyFormat = Format['HTML']
        try:  
            if error == 1:
                http = 'mailError.html'
            else:
                http = 'mail.html'
            fd = io.open(os.path.join(os.path.join(basePath,'httpBodyMail'),http), 'r', encoding="utf8")    
        except Exception:
            print('No se puede abrir archivo.')
            return        
        mensajehttp = fd.read()
        fd.close()
        mensajehttp = mensajehttp.replace("{mensaje}", mensaje)
        mail.HTMLBody = mensajehttp
        if adjunto is None:
            mail.Send()
        else:
            mail.Attachments.Add(Source=adjunto)
            mail.Send()
        # end if
        print_('Email de finalizacion del proceso enviado')
    except:
        print_('Ya que se ejecuta en OS diferente a Windows no se envia mensaje de finalizacion.')
   
    


#%%
        

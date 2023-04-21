import pandas as pd
from telegram.ext import Updater, CommandHandler, CallbackContext
from datetime import datetime
from credenciales import token,key, gc
import logging

# Configurar el registro de errores
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.ERROR)
def error(update, context):
    """Manejar las excepciones"""
    logging.error(f'Error: {context.error}')
    update.message.reply_text('Hubo un error interno. Pero me recupere y sigo trabajando')
contexto = CallbackContext
def actualizar(contexto):
  global BD  
  global ASIS_OP
  global ina
  global hoy 
  global faltas 
  global ina
  global per
  global rep
  global vac
  global VACANTES
  global anti_join
  global INCIDENCIAS
  global rows
  global todos
  global excelentes
  global malos
  global FIN_SUP
  global FIN_OP
  global regulares
  global buenos
  global ASISTENCIA_NEW
  global BD_NEW
  # fecha de hoy
  dia = datetime.today()
  hoy = dia.strftime('%Y-%m-%d')

  # ASISTENCIA OPERARIOS
  rows = gc.open_by_key(key).worksheet("ASIS_OP").get_all_records()
  ASIS_OP = pd.DataFrame(rows)
  ASIS_OP['Date'] = pd.to_datetime(ASIS_OP['Date'], format='%d/%m/%Y')
  ASIS_OP = ASIS_OP.set_index("id_asistencia")
  ASISTENCIA_NEW = ASIS_OP.loc[:,['Date','Operario','Regional','Agencia','Asistencia']]
  # DF_HOY Muestra la carga de asistencia del dia 
  DF_HOY = ASIS_OP[ASIS_OP['Date'] == hoy]
 
   # INCIDENCIAS Muestra las Incidencias diarias (Reposos, Permisos, Inasistencias)
  INCIDENCIAS = DF_HOY[DF_HOY['Asistencia'] != 'Asistente' ]
  faltas = INCIDENCIAS.loc[:, ["Operario","Regional","Agencia","Asistencia","Novedad"] ]
  # todas las incidencias
  faltas = faltas.sort_values(['Regional','Operario'])
  # Todas las inasistencias
  ina = faltas[faltas['Asistencia'] == 'Inasistente']
  # Todas las faltas justificadas
  per = faltas[faltas['Asistencia'] == 'Permiso']
  # todos los reposos
  rep = faltas[faltas['Asistencia'] == 'Reposo']
  #vacaciones
  vac = faltas[faltas['Asistencia'] == 'Vacaciones']
    
  # BD BASE DE DATOS  
  BD = gc.open_by_key(key).worksheet("DATOS").get_all_records()
  BD = pd.DataFrame(BD)
  BD_NEW = BD.loc[:,['ID_OPERARIO','OPERARIO','CEDULA', 'STATUS', 'REGIONAL', 'AGENCIA','COORDINADOR','SUPERVISOR', 'FECHA']]
  # VACANTES
  VACANTES = BD.loc[: ,['ID_OPERARIO','OPERARIO','REGIONAL','AGENCIA']]
  VACANTES = VACANTES.sort_values(['REGIONAL','OPERARIO'])
  VACANTES = VACANTES[VACANTES['OPERARIO'] == 'PENDIENTE POR INGRESO']
  VACANTES = VACANTES.loc[:,['ID_OPERARIO','REGIONAL','AGENCIA']]  
  
  # FILTRADO DE COLUMNAS DE LA BD
  BD = BD.loc[:,['OPERARIO','REGIONAL','AGENCIA']]

  # FALTANTES POR CARGAR
  outer = pd.merge(BD, DF_HOY, left_on='OPERARIO', right_on='Operario',how='outer', indicator=True)
  anti_join = outer[(outer._merge=='left_only')].drop('_merge', axis=1)
  anti_join = anti_join[(anti_join.OPERARIO != 'CERRADA') & (anti_join.OPERARIO != 'PENDIENTE POR INGRESO')]
  anti_join = anti_join.loc[:,['OPERARIO','REGIONAL','AGENCIA']]
  anti_join = anti_join.sort_values(['REGIONAL','OPERARIO'])

  # FIN DE SEMANA OPERARIOS
  FIN_OP = gc.open_by_key(key).worksheet("FIN_SEMANA").get_all_records()    
  FIN_OP = pd.DataFrame(FIN_OP)
  FIN_OP['FECHA'] = pd.to_datetime(FIN_OP['FECHA'], format='%d/%m/%Y')
  FIN_OP = FIN_OP[FIN_OP['FECHA'] == hoy]
  FIN_OP = FIN_OP.loc[:,['NOMBRE', 'ASISTENCIA']]

  # FIN DE SEMANA SUPERVISORES
  FIN_SUP = gc.open_by_key(key).worksheet("FIN_SUPERVISORES").get_all_records()    
  FIN_SUP = pd.DataFrame(FIN_SUP)
  FIN_SUP['FECHA'] = pd.to_datetime(FIN_SUP['FECHA'], format='%d/%m/%Y')
  FIN_SUP = FIN_SUP[FIN_SUP['FECHA'] == hoy]
  FIN_SUP = FIN_SUP.loc[:,['NOMBRE', 'ASISTENCIA']]

  # EVALUACIONES
  form = gc.open_by_key(key).worksheet("Respuestas de formulario 1").get_all_records()
  evaluaciones = pd.DataFrame(form)
  evaluaciones['Fecha'] = pd.to_datetime(evaluaciones['Fecha'], format='%d/%m/%Y')
  evealuaciones_daily = evaluaciones[evaluaciones["Fecha"] == hoy]
  evaluaciones_daily = evealuaciones_daily.loc[:,['Fecha','SUPERVISOR','REGIONAL','AGENCIA O PISO DE TORRE','OPERARIO','EVALUACION DEL OPERARIO [PASILLOS]','EVALUACION DEL OPERARIO [OFICINAS Y MOBILIARIO]','EVALUACION DEL OPERARIO [VIDRIOS]','EVALUACION DEL OPERARIO [BAÑOS]','EVALUACION DEL OPERARIO [COMEDOR]']]
  renombre = {'Fecha' : 'FECHA', 'AGENCIA O PISO DE TORRE' : 'AGENCIA', 'EVALUACION DEL OPERARIO [PASILLOS]' : 'PASILLOS','EVALUACION DEL OPERARIO [OFICINAS Y MOBILIARIO]': 'OFICINAS', 'EVALUACION DEL OPERARIO [VIDRIOS]' : 'VIDRIOS', 'EVALUACION DEL OPERARIO [BAÑOS]' : 'BAÑOS', 'EVALUACION DEL OPERARIO [COMEDOR]' : 'COMEDOR'}
  evaluaciones_update = evaluaciones_daily.rename(columns=renombre)
  numeros = ['PASILLOS','OFICINAS', 'VIDRIOS','BAÑOS','COMEDOR']
  for i in numeros:
    evaluaciones_update[i] = pd.to_numeric(evaluaciones_update[i])
  promedio = evaluaciones_update[numeros]
  evaluaciones_update['promedio'] = promedio.mean(axis=1)/4
  todos = evaluaciones_update.loc[:,['OPERARIO','REGIONAL','AGENCIA', 'promedio']].sort_values(['REGIONAL','OPERARIO'])
  malos = todos[todos['promedio'] <= 0.5].sort_values(['REGIONAL','OPERARIO'])
  malos['promedio'] = malos['promedio'].apply(lambda x: '{:.2f}%'.format(x * 100))
  regulares = todos[(todos['promedio'] > 0.5) & (todos['promedio'] <= 0.8)].sort_values(['REGIONAL','OPERARIO'])
  regulares['promedio'] = regulares['promedio'].apply(lambda x: '{:.2f}%'.format(x * 100))
  buenos = todos[(todos['promedio'] > 0.8) & (todos['promedio'] <= 0.95)].sort_values(['REGIONAL','OPERARIO'])
  buenos['promedio'] = buenos['promedio'].apply(lambda x: '{:.2f}%'.format(x * 100))
  excelentes = todos[todos['promedio'] == 1].sort_values(['REGIONAL','OPERARIO'])
  excelentes['promedio'] = excelentes['promedio'].apply(lambda x: '{:.2f}%'.format(x * 100))
  todos['promedio'] = todos['promedio'].apply(lambda x: '{:.2f}%'.format(x * 100))

# ACTUALIZAR CAMBIOS EN LA BD


# Comienza el bot 

def start(update, context):
    chat_id = update.message.chat_id
    photo_path = "slogan.png" # Ruta de la imagen que quieres enviar
    photo = open(photo_path, 'rb')
    context.bot.send_photo(chat_id=chat_id, photo=photo, caption= '<b>Menu de opciones 📣</b>\n\n'\
        'Selecciona cualquier opcion para Consultar:\n\n'\
        '1. ⭕️ /Incidencias Inasistencias, Reposos, Permisos y Vacaciones\n\n'\
        '2. ❎ /Faltantes por cargar del dia de hoy \n\n'\
        '3. 📈 /Evaluaciones del operario\n\n'\
        '4. 📅 /Visitas a las agencias del dia de hoy \n\n'\
        '5. 🔎 /Fin_semana de los Supervisores y Operarios\n\n'\
        '6. ⚠️ /Vacantes disponibles\n\n'\
        '7. 📈 /copiar_datos envia la informacion de las faltas del dia a una hoja de calculo\n\n'\
        '8. 📨 /Archivo_bolsas Se crea un archivo de operarios a recibir Bolsas\n\n'\
        '9. 📊 /Dashboard Muestra el Dashboard de los operarios \n\n', parse_mode='HTML')

  
def copiar_datos(update, context):
  # Autenticar la conexión con las credenciales proporcionadas
    # Obtener la hoja de cálculo correspondiente
  sh = gc.open('hoja para hacer push').worksheet('hoja de actualizaciones')
  # Obtener la hoja de trabajo donde deseas agregar los datos
  
  ASIS_OP = pd.DataFrame(rows)
  ASIS_OP['Date'] = pd.to_datetime(ASIS_OP['Date'], format='%d/%m/%Y')
  dia = datetime.today()
    # Convertimos el objeto datetime a string con formato <día>/<mes>/<año>
  hoy = dia.strftime('%Y-%m-%d')
    # DF_HOY Muestra la carga de asistencia del dia 
  DF_HOY = ASIS_OP[ASIS_OP['Date'] == hoy]
    # INCIDENCIAS Muestra las Incidencias diarias (Reposos, Permisos, Inasistencias)
  INCIDENCIAS = DF_HOY[DF_HOY['Asistencia'] != 'Asistente' ]
  DF = INCIDENCIAS.loc[:,['Date', 'Operario','Coordinador','Supervisor','Regional','Agencia','Asistencia']]
  DF['Date'] = pd.to_datetime(DF['Date'], format='%d-%m-%Y')
  DF = DF[(DF['Date'] == hoy) & (DF['Asistencia'] !='Asistente')] 
  DF['Date'] = DF['Date'].apply(lambda x: x.strftime('%d-%m-%Y'))
  empujar = DF.sort_values(['Regional', 'Operario'])
  empujar = empujar.values.tolist()
  sh.append_rows(empujar)
  context.bot.send_message(chat_id=update.effective_chat.id, text='Los datos se han copiado correctamente')

def Fin_semana(update, context):
  VER = 'Muestra la asistencia de los Supervisores y Operarios el fin de semana:\n\n' + '1. ✅ /Fin_supervisores Muestra los Supervisores cargados hoy\n\n' + '2. ✅ /Fin_Operarios Muestra los Operarios Cargados hoy \n\n'
  update.message.reply_text(VER)

def Fin_supervisores(update, context):
  global FIN_SUP
  if FIN_SUP.empty == False:
      for i in range(len(FIN_SUP)):
          titulo1 = f"Supervisores cargados hoy {hoy} ✅\n" 
          INCIDENCIAS1= titulo1 + '\n El supervisor: ' + str((FIN_SUP['NOMBRE'].iloc[i])) +' está ' + str((FIN_SUP['ASISTENCIA'].iloc[i]))
          update.message.reply_text(INCIDENCIAS1)
  else:
    no = f"Tal vez la fecha de hoy {hoy} no corresponde a un fin de semana intenta consultarlo un sabado o un domingo"
    update.message.reply_text(no)


def Fin_operarios(update, context):
  global FIN_OP
  if FIN_OP.empty == False:
      for i in range(len(FIN_OP)):
          titulo1 = f"Operarios cargados hoy {hoy} ✅\n" 
          INCIDENCIAS1= titulo1 + '\n Operario: ' + str((FIN_OP['NOMBRE'].iloc[i])) +'\n Status: ' + str((FIN_OP['ASISTENCIA'].iloc[i]))
          update.message.reply_text(INCIDENCIAS1)
  else:
    no = f"Tal vez la fecha de hoy {hoy} no corresponde a un fin de semana intenta consultarlo un sabado o un domingo"
    update.message.reply_text(no)

def Incidencias(update, context):
  VER = 'Que tipo de incidencias quieres ver:\n\n' + '1. 🔴 /Inasistencias Muestra las faltas Injustificadas\n\n' + '2. 🟠 /Permisos Otorgados por coordinadores \n\n' + '3. 🟡 /Reposos Muestra Operarios de Reposos \n\n' + '3. ❕ /Vacaciones Muestra Operarios de Vacaciones \n\n'+ '4. 🚦 /Todas Muestra todas las Incidencias del dia \n\n' + '5. ↩️ /start Presione Para Volver al MENU principal \n\n'
  update.message.reply_text(VER)

def Inasistencias(update, context):
  global ina
  if ina.empty == False:
      for i in range(len(ina)):
          titulo1 = f"Incidencias del Día {hoy}" 
          INCIDENCIAS1= titulo1 + '\n Operario: ' + str((ina['Operario'].iloc[i])) +'\n Regional: ' + str((ina['Regional'].iloc[i])) +'\n Agencia: ' + str((ina['Agencia'].iloc[i])) + '\n Tipo de falta: ' + '🔴 ' + str((ina['Asistencia'].iloc[i])) + '\n Novedad: ' + str((ina['Novedad'].iloc[i]))
          update.message.reply_text(INCIDENCIAS1)
  else:
    no = f"✅ Personal sin inasistencias del día {hoy}"
    update.message.reply_text(no)

def Permisos(update, context):
  global per
  if per.empty == False:
    for i in range(len(per)):
      titulo1 = f"Incidencias del Día {hoy} 📣 " 
      INCIDENCIAS2= titulo1 + '\n Operario: ' + str((per['Operario'].iloc[i])) +'\n Regional: ' + str((per['Regional'].iloc[i])) +'\n Agencia: ' + str((per['Agencia'].iloc[i])) + '\n Tipo de falta: '  + '🟠 '+ str((per['Asistencia'].iloc[i])) + '\n Novedad: ' + str((per['Novedad'].iloc[i]))
      update.message.reply_text(INCIDENCIAS2)
  else:
    no = f"✅ Personal sin permisos del día {hoy}"
    update.message.reply_text(no)
     
def Reposos(update, context):
  global rep
  if rep.empty == False:
    for i in range(len(rep)):
      titulo1 = f"Incidencias del Día {hoy}" 
      INCIDENCIAS3= titulo1 + '\n Operario: ' + str((rep['Operario'].iloc[i])) +'\n Regional: ' + str((rep['Regional'].iloc[i])) +'\n Agencia: ' + str((rep['Agencia'].iloc[i])) + '\n Tipo de falta: ' + '🟡 ' + str((rep['Asistencia'].iloc[i])) + '\n Novedad: ' + str((rep['Novedad'].iloc[i]))
      update.message.reply_text(INCIDENCIAS3)
  else:
    no = f"✅ Personal sin Reposos del día {hoy}"
    update.message.reply_text(no)

def Vacaciones(update, context):
  global vac
  if vac.empty == False:
    for i in range(len(vac)):
      titulo1 = f"Incidencias del Día {hoy}" 
      INCIDENCIAS4= titulo1 + '\n Operario: ' + str((vac['Operario'].iloc[i])) +'\n Regional: ' + str((vac['Regional'].iloc[i])) +'\n Agencia: ' + str((vac['Agencia'].iloc[i])) + '\n Tipo de falta: ' + '❕ ' + str((vac['Asistencia'].iloc[i]))
      update.message.reply_text(INCIDENCIAS4)
  else:
    no = f"✅ Personal sin Reposos del día {hoy}"
    update.message.reply_text(no)

def Todas(update, context):
  global faltas
  if faltas.empty == False:
    for i in range(len(faltas)):
      titulo1 = f"Incidencias del Día {hoy}" 
      INCIDENCIAS= titulo1 + '\n Operario: ' + str((faltas['Operario'].iloc[i])) +'\n Regional: ' + str((faltas['Regional'].iloc[i])) +'\n Agencia: ' + str((faltas['Agencia'].iloc[i])) + '\n Tipo de falta: ' + str((faltas['Asistencia'].iloc[i])) + '\n Novedad: ' + str((faltas['Novedad'].iloc[i]))
      update.message.reply_text(INCIDENCIAS)
  else:
    no = f"✅ Personal del día {hoy} completo"
    update.message.reply_text(no)

def Evaluaciones(update, context):
  VER = 'Que operarios evaluados hoy quieres ver:\n\n' + '1. ✅ /Todos los operarios evaluados hoy \n\n' + '2. 🟢 /Excelentes Muestra los mejores Operarios de hoy \n\n' + '3. 🔵 /Buenos operarios evaluados de hoy \n\n' + '4. 🟡 /Regulares operarios evaluados de hoy \n\n' + '5. 🔴 /Malos Muestra Los peores evaluados de hoy \n\n'
  update.message.reply_text(VER)

def Todos(update, context):
  global todos
  if todos.empty == False:
    for i in range(len(todos)):
      titulo1 = f"Evaluaciones del dia {hoy}" 
      INCIDENCIAS1= titulo1 + '\n Operario: ' + str((todos['OPERARIO'].iloc[i])) +'\n Regional: ' + str((todos['REGIONAL'].iloc[i])) +'\n Agencia: ' + str((todos['AGENCIA'].iloc[i])) + '\n Desempeño: ' + str((todos['promedio'].iloc[i])) + " ✅"
      update.message.reply_text(INCIDENCIAS1)
  else:
    empty = f"✅ Los operarios del dia {hoy} aun no han sido evaluados"
    update.message.reply_text(empty)

def Excelentes(update, context):
  global excelentes
  if excelentes.empty == False:
    for i in range(len(excelentes)):
      titulo1 = f"Evaluaciones Excelentes del dia {hoy}" 
      INCIDENCIAS1= titulo1 + '\n Operario: ' + str((excelentes['OPERARIO'].iloc[i])) +'\n Regional: ' + str((excelentes['REGIONAL'].iloc[i])) +'\n Agencia: ' + str((excelentes['AGENCIA'].iloc[i])) + '\n Desempeño: ' + str((excelentes['promedio'].iloc[i])) + " 🟢"
      update.message.reply_text(INCIDENCIAS1)
  else:
    empty = f"✅ No hay operararios del dia {hoy} con evaluacion perfecta"
    update.message.reply_text(empty)

def Buenos(update, context):
  global buenos
  if buenos.empty == False:
    for i in range(len(buenos)):
      titulo1 = f"Evaluaciones buenas del dia {hoy}" 
      INCIDENCIAS1= titulo1 + '\n Operario: ' + str((buenos['OPERARIO'].iloc[i])) +'\n Regional: ' + str((buenos['REGIONAL'].iloc[i])) +'\n Agencia: ' + str((buenos['AGENCIA'].iloc[i])) + '\n Desempeño: ' + str((buenos['promedio'].iloc[i])) + " 🔵"
      update.message.reply_text(INCIDENCIAS1)
  else:
    empty = f"✅ No hay operararios del dia {hoy} con buena evaluacion"
    update.message.reply_text(empty)

def Regulares(update, context):
  global regulares
  if regulares.empty == False:
    for i in range(len(regulares)):
      titulo1 = f"Evaluaciones regulares del dia {hoy}" 
      INCIDENCIAS1= titulo1 + '\n Operario: ' + str((regulares['OPERARIO'].iloc[i])) +'\n Regional: ' + str((regulares['REGIONAL'].iloc[i])) +'\n Agencia: ' + str((regulares['AGENCIA'].iloc[i])) + '\n Desempeño: ' + str((regulares['promedio'].iloc[i])) + " 🟡"
      update.message.reply_text(INCIDENCIAS1)
  else:
    empty = f"✅ No hay operararios del dia {hoy} con evaluacion regular"
    update.message.reply_text(empty)


def Malos(update, context):
  global malos
  if malos.empty == False:
    for i in range(len(malos)):
      titulo1 = f"Evaluaciones de mal desempeño del dia {hoy}" 
      INCIDENCIAS1= titulo1 + '\n Operario: ' + str((malos['OPERARIO'].iloc[i])) +'\n Regional: ' + str((malos['REGIONAL'].iloc[i])) +'\n Agencia: ' + str((malos['AGENCIA'].iloc[i])) + '\n Desempeño: ' + str((malos['promedio'].iloc[i])) + "🔴"
      update.message.reply_text(INCIDENCIAS1)
  else:
    empty = f"✅ No hay operararios del dia {hoy} con evaluacion por debajo del 50%"
    update.message.reply_text(empty)

def Faltantes(update, context):
  foul = 'Ver el nombre de los operarios faltantes o el numero de faltantes:\n\n' + '1. 🔤 /Nombres de los operarios faltantes por cargar hoy \n\n' + '2. 🔢 /Numeros de Operarios faltantes por cargar hoy \n\n'
  update.message.reply_text(foul)

    
def Nombres(update, context):
  global anti_join
  if anti_join.empty == False:
    for i in range(len(anti_join)):
      titulo2 = f"❌ Nombre de operarios faltantes por Cargar hoy {hoy}"
      FALTANTES =titulo2 + '\n Operario: ' + str((anti_join['OPERARIO'].iloc[i])) + '\n Regional: ' + str((anti_join['REGIONAL'].iloc[i])) + '\n Agencia: ' + str((anti_join['AGENCIA'].iloc[i]))
      update.message.reply_text(FALTANTES)
  else:
    empty = f"Todos los operarios del dia {hoy} fueron cargados exitosamente ✅"
    update.message.reply_text(empty)

def Numeros(update, context):
  global anti_join
  anti_join = anti_join.loc[:,['REGIONAL','OPERARIO']].groupby('REGIONAL').count().reset_index()
  if anti_join.empty == False:
    for i in range(len(anti_join)):
      titulo2 = f"❌ Numero de operarios faltantes por regional {hoy}\n"
      FALTANTES =titulo2  + '\n Regional: ' + str((anti_join['REGIONAL'].iloc[i])) + '\n Numero de operarios faltantes: ' +  str((anti_join['OPERARIO'].iloc[i])) 
      update.message.reply_text(FALTANTES)
  else:
    empty = f"✅ Todos los operarios del dia {hoy} fueron cargados exitosamente"
    update.message.reply_text(empty)

def Vacantes(update, contex):
  global VACANTES
  if VACANTES.empty == False:
    for i in range(len(VACANTES)):
      titulo3 = f"⚠️ Vacantes actuales al dia {hoy}"
      por_ingreso =titulo3 + '\n ID: ' + str((VACANTES['ID_OPERARIO'].iloc[i])) + '\n Regional: ' + str((VACANTES['REGIONAL'].iloc[i]))  + '\n Agencia : ' + str((VACANTES['AGENCIA'].iloc[i])) 
      update.message.reply_text(por_ingreso)
  else:
    no_vacantes = f"Al dia {hoy} no hay vacantes Disponibles"
    update.message.reply_text(no_vacantes)

def Dashboard(update, context):

  URL = 'Aqui Podras visualizar del Dashboard de Gestion de los operarios 📊 \n\n' + 'https://lookerstudio.google.com/reporting/def9c71a-2027-46c9-851e-0b11e8223177/page/mjKDD'
  update.message.reply_text(URL)

def Archivo_bolsas(update, context):
  context.bot.send_message(chat_id=update.effective_chat.id, text='<b>ATENCION Para generar el archivo de las bolsas debes seguir los siguientes pasos:📣</b>\n\n'+
        '1.  Vas a escribir el comando \Bolsas seguido de 3 argumentos con espacios Los argumentos son:\n\n'+
        '2. fecha de inicio, fecha final y fecha de tolerancia\n\n'+
        '3.  "fecha de inicio" es el primer dia a tomar en cuenta para las bolsas de dicho mes\n\n'+
        '4.  "fecha final" es el ultimo dia a tomar en cuenta para las bolsas de dicho mes\n\n'+
        '5.  "fecha de tolerancia" es el dia maximo a tomar en cuenta de un operario nuevo ingreso es decir: \n\n'+
        '5.1 Si las bolsas de marzo se entregan el 12 de abril la tolerancia es que ese operario debio ingresar antes del 11 de marzo para recibir bolsa\n\n'+
        '6.  Sabiendo Esto entonces debemos tomar en cuenta que las fechas se escriben en un formato donde comienza por el año-mes-dia\n\n'+
        '7.  Quedaria de esta forma YYYY-MM-DD \n\n'+
        '8. Aqui tienes un ejemplo de como generar el archivo de las bolsas sabiendo todo esto ejemplificando con el mes de marzo:\n\n\n'+
        '/Bolsas 2023-03-01 2023-03-31 2023-03-10\n\n\n'
        '<b>NOTA: Esto generará un archivo en GOOGLE SHEET con las personas que reciben bolsas en ese mes en la hoja de calculo llamada BOLSAS separando cada pestaña de la hoja por cada capital</b>', parse_mode='HTML')

  
def Bolsas(update, context):
  global ASISTENCIA_NEW
  global BD_NEW
  BD_NEW['FECHA'] = pd.to_datetime(BD_NEW['FECHA'], format='%d/%m/%Y')
  inicio = context.args[0]
  final = context.args[1]
  fecha_ingreso = context.args[2]
  inicio = datetime.strptime(inicio, '%Y-%m-%d')
  final = datetime.strptime(final, '%Y-%m-%d')
  fecha_ingreso = datetime.strptime(fecha_ingreso, '%Y-%m-%d')
  data_mes = ASISTENCIA_NEW[(ASISTENCIA_NEW['Date'].apply(lambda x: datetime.strptime(x.strftime('%Y-%m-%d'), '%Y-%m-%d')) >= inicio) & (ASISTENCIA_NEW['Date'].apply(lambda x: datetime.strptime(x.strftime('%Y-%m-%d'), '%Y-%m-%d')) <= final)]
  asistencia = data_mes.groupby(['Operario', 'Asistencia']).size().unstack(fill_value=0)
  asistencia = asistencia.reset_index()
  noreciben_bolsa = asistencia[asistencia['Inasistente']  >=3] 
  noreciben_bolsa = noreciben_bolsa.loc[:,['Operario','Asistente','Inasistente']]

  # hoja donde se almacenaran las datas de las bolsas
  hoja_calculo = gc.open('BOLSAS CAPITALES')
  # Limpiar las hoja de cálculo
  limpiar = ["CAPITALES", "NO RECIBEN"]
  for limpiando in limpiar:
    hoja = hoja_calculo.worksheet(limpiando)
    hoja.clear()
  # rellenar una vez limpia
  columns = noreciben_bolsa.columns.tolist()
  valores = noreciben_bolsa.values.tolist()
  datos = [columns] + valores
  hoja_calculo.worksheet("NO RECIBEN").update('A1', datos)
  # enviar no reciben bolsas 
  base = BD_NEW[BD_NEW['FECHA'] <= fecha_ingreso]
  outer = pd.merge(base, noreciben_bolsa, left_on='OPERARIO', right_on='Operario',how='outer', indicator=True)
  bolsa = outer[(outer._merge=='left_only')].drop('_merge', axis=1)
  bolsa = bolsa.loc[:,['OPERARIO','CEDULA','REGIONAL','COORDINADOR','SUPERVISOR']]
  df = bolsa[(bolsa["OPERARIO"] != "CERRADA") & (bolsa["OPERARIO"] != "PENDIENTE POR INGRESO")].sort_values(['REGIONAL', 'CEDULA'])
  datos_df = df.values.tolist()
  columnas_df = df.columns.tolist()
  datos2 = [columnas_df] + datos_df
  hoja_calculo.worksheet("CAPITALES").update('A1', datos2)
  longitud = len(df)
  #diccionario vacio
  groups = {}
# Iteramos sobre los grupos y agregamos los datos correspondientes al DataFrame correspondiente
  regionales = df['REGIONAL'].unique()
  for group in regionales:
      group_df = df[df['REGIONAL'] == group]
      groups[group] = group_df
# hoja a eliminar
  eliminar = ["ALEDAÑOS", "CAPITAL 1", "CAPITAL 2", "CAPITAL 3", "CAPITAL 4", "CAPITAL 5", "SEDE ADMINISTRATIVA","TORRE PRINCIPAL"]
  # Eliminamos cada hoja de cálculo existente excepto la que se desea conservar
  for hoja_nombre in eliminar:
      hoja = hoja_calculo.worksheet(hoja_nombre)
      hoja_calculo.del_worksheet(hoja)
  # Creamos una hoja de cálculo para cada DataFrame y agregamos los datos a cada hoja
  for group, group_df in groups.items():
      worksheet = hoja_calculo.add_worksheet(title=group, rows=len(group_df), cols=len(group_df.columns))
      cell_list = worksheet.range(1, 1, len(group_df)+1, len(group_df.columns))
      for cell in cell_list:
          if cell.row == 1:
              cell.value = group_df.columns[cell.col - 1]
          else:
              cell.value = group_df.iloc[cell.row - 2, cell.col - 1]
      worksheet.update_cells(cell_list)
  context.bot.send_message(chat_id=update.effective_chat.id, text=f'Archivo-Bolsa de Capitales generado correctamente para {longitud} operarios ✅\n\n' + 'Podras visualizarlo aqui:\n\n' + 'https://docs.google.com/spreadsheets/d/1DRqLb9o1ljsyCBV7h5loxl557rnjW_c_wiu0SeXfiVA/edit?usp=sharing')

if __name__=='__main__':
  updater = Updater(token, use_context=True)
  dp = updater.dispatcher
  dp.add_handler(CommandHandler('start', start))
  dp.add_handler(CommandHandler('Incidencias', Incidencias))
  dp.add_handler(CommandHandler('Inasistencias', Inasistencias))
  dp.add_handler(CommandHandler('Reposos', Reposos))
  dp.add_handler(CommandHandler('Permisos', Permisos))
  dp.add_handler(CommandHandler('Todas', Todas))
  dp.add_handler(CommandHandler('Vacaciones', Vacaciones))
  dp.add_handler(CommandHandler('Todos', Todos))
  dp.add_handler(CommandHandler('Excelentes', Excelentes))
  dp.add_handler(CommandHandler('Buenos', Buenos))
  dp.add_handler(CommandHandler('Regulares', Regulares))
  dp.add_handler(CommandHandler('Malos', Malos))
  dp.add_handler(CommandHandler('copiar_datos', copiar_datos))
  dp.add_handler(CommandHandler('Faltantes', Faltantes))
  dp.add_handler(CommandHandler('Numeros', Numeros))
  dp.add_handler(CommandHandler('Nombres', Nombres))
  dp.add_handler(CommandHandler('Evaluaciones', Evaluaciones))
  dp.add_handler(CommandHandler('Fin_semana', Fin_semana))
  dp.add_handler(CommandHandler('Fin_supervisores', Fin_supervisores))
  dp.add_handler(CommandHandler('Fin_operarios', Fin_operarios))
  dp.add_handler(CommandHandler('Vacantes', Vacantes))
  dp.add_handler(CommandHandler('Dashboard', Dashboard))
  dp.add_handler(CommandHandler('Archivo_bolsas', Archivo_bolsas))
  dp.add_handler(CommandHandler('Bolsas', Bolsas, pass_args=True))
  dp.add_error_handler(error)
  actualizar(contexto)
  job_queue = updater.job_queue
  job_queue.run_repeating(actualizar, interval=300, first=0)
  updater.start_polling()
  print('Estoy Corriendo cada 10 minutos')
  updater.idle()
  
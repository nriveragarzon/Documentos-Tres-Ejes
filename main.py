#######################
# 0. Importar librerias
#######################
import streamlit as st
# Selectores
import selectores as selectores
# Datos
import datos as dat
# Documentos
import documentos as doc
# Descarga
import descarga as desc
import base64

# # Configuración página web
st.set_page_config(page_title="Documentos Tres Ejes", page_icon = ':bar_chart:', layout="wide",  initial_sidebar_state="expanded")

# Secrets
def cargar_contraseñas(nombre_archivo):
    return st.secrets
cargar_contraseñas(".streamlit/secrets.toml")

# Datos de sesión de Snowflake
connection = st.connection("snowflake")
sesion_activa = connection.session()

# Limpiar cache
def limpiar_cache():
    """
    Limpia el cache de datos de Streamlit.
    """
    st.cache_data.clear()  # Limpia el cache de datos de Streamlit

# Imágenes
# Aplicación
procolombia_img = 'Insumos/PRO_PRINCIPAL_HORZ_PNG.png'
mincit_img = 'Insumos/Logo MinCit_Mesa de trabajo 1.png'
footer = 'Insumos/Footer.jpg'
banner = 'Insumos/Banner.jpg'
# Documento
top_left_img = 'Insumos/doc_top_left.png'
bottom_right = 'Insumos/doc_bottom_right.png'



######################################
# 1. Definir el flujo de la aplicación
######################################

def main():
    ## Menú de navegación
    ### Logo ProColombia
    with st.sidebar:
        st.image(procolombia_img, caption=None, use_column_width="always")
        st.markdown('#')     
    ## Páginas
    ### Opciones con iconos
    options = {
        "Portada :arrow_backward:": "Portada",
        "Documentos :arrow_backward:": "Documentos",
        "Fuentes :arrow_backward:": "Fuentes"
    }
    #### Configuración del sidebar
    page = st.sidebar.radio("Elija una página", list(options.keys()))
    selected_option = options.get(str(page))  # Usar get para manejar None de manera segura
    if selected_option:
        if selected_option == "Portada":
            page_portada()
        elif selected_option == "Documentos":
            documentos()
        elif selected_option == "Fuentes":
            page_fuentes()
    ### Logo MINCIT
    with st.sidebar:
        ### Elaborado por la Coordinación de Analítica
        st.markdown('#')
        st.subheader("Elaborado por:") 
        st.write("Coordinación de Analítica, Gerencia de Inteligencia Comercial, ProColombia.") 
        st.markdown('#')
        st.image(mincit_img, caption=None, use_column_width="always")
        
        
###########################
# Personalización de estilo
###########################

# Función para cargar el CSS desde un archivo
def load_css(file_name):
    with open(file_name) as f:
        return f.read()

# Cargar y aplicar el CSS personalizado
css = load_css("styles.css")
st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)

##############
# Contenido
##############

#########
# Portada
#########

def page_portada():
    # Banner
    st.image(image=banner, caption=None, use_column_width="always")

    # Contenido de la portada
    st.markdown("""
    <style>
    .justify-text {
        text-align: justify;
    }
    .justify-text p, .justify-text div {
        margin-bottom: 10px; /* Espacio entre los párrafos */
    }
    .justify-text .indent {
        margin-left: 20px; /* Ajusta este valor para cambiar la indentación del texto */
    }
    </style>
    <h1>Documentos Tres Ejes</h1>
    <h2>Bienvenido a la Aplicación de Documentos Tres Ejes</h2>            
    <h2>¿Qué puede hacer con esta aplicación?</h2>
    <div class="justify-text">
        <p> Esta aplicación le permitirá generar y descargar a demanda informes detallados que resuman las cifras más relevantes de los tres ejes de negocio de ProColombia. Diseñada para ofrecer una experiencia intuitiva y eficiente, la plataforma le facilita el acceso a datos cruciales organizados según su necesidad específica: ya sea por continente, HUB, tratado de libre comercio (TLC), país, Colombia o departamento. Simplemente elija el nivel de agrupación, seleccione la opción específica que le interesa, y en cuestión de segundos podrá descargar el informe en formato Word, PDF o Excel. Optimice su análisis y toma de decisiones con documentos precisos y personalizados. </p>
    </div>
    <h2>Instrucciones para el Uso de la Aplicación</h2>
    <div class="justify-text">
        <p class="indent"><strong>1. Navegación:</strong> Use el menú de navegación en la barra lateral para acceder a las diferentes secciones de la aplicación.</p>
        <p class="indent"><strong>2. Documentos:</strong> Elija los filtros que desee y haga clic en el botón correspondiente para generar y descargar su informe.</p>
        <p class="indent"><strong>3. Fuentes:</strong> Explore las fuentes de información de los informes personalizados a los que puede acceder.</p>
    </div>
    <h2>Soporte</h2>
    <div class="justify-text">
        <p>Si tiene alguna pregunta o necesita asistencia, no dude en ponerse en contacto con el equipo de la Coordinación de Analítica, Gerencia de Inteligencia Comercial, ProColombia.</p>
    </div>               
    """, unsafe_allow_html=True)
    # Footer
    st.image(image=footer, caption=None, use_column_width="always")

############
# Documentos
############

def documentos():
    # Banner
    st.image(image=banner, caption=None, use_column_width="always")
    # Instrucciones de descarga
    st.markdown("""
    <style>
    .justify-text {
        text-align: justify;
    }
    .justify-text p, .justify-text div {
        margin-bottom: 10px; /* Espacio entre los párrafos */
    }
    .justify-text .indent {
        margin-left: 20px; /* Ajusta este valor para cambiar la indentación del texto */
    }
    </style>
    <h1>Documentos Tres Ejes</h1>              
    <h2>Descarga de documentos</h2>
    <div class="justify-text">
        <h3>Pasos para descargar documentos</h3>
            <p class="indent"><strong>1. Elija el nivel de agrupación del informe que desea:</strong> Seleccione una de las opciones disponibles (Continente, HUB, TLC, País, Colombia, Departamento) para obtener un informe a nivel agregado.</p>
            <p class="indent"><strong>2. Seleccione una opción específica:</strong> Una vez haya elegido el nivel de agrupación, la aplicación le permitirá elegir un continente, HUB, TLC, país o departamento específico según la opción seleccionada en el punto 1.</p>
            <p class="indent"><strong>3. Espere unos segundos:</strong> La aplicación procesará su solicitud y después de 45 segundos le habilitará tres botones de descarga.</p>
            <p class="indent"><strong>4. Descargue el documento:</strong> Haga clic en el botón correspondiente para descargar el archivo en el formato deseado (Word o PDF). También puede descargar un archivo Excel con los datos del informe.</p>
    </div>
    <h2>Empiece aquí</h2>
    <div class="justify-text">
    <p>Elija entre las siguientes opciones para empezar el proceso.</p>
    </div>               
    """, unsafe_allow_html=True)
    # Elección del usuario entre diferentes agrupaciones de datos
    eleccion_usuario = st.radio("Seleccione una opción", 
                                # Opciones: 
                                ('**Continente:** Explore un informe organizado por continente a nivel mundial.', 
                                 '**HUB:** Explore un informe organizado por HUB.',
                                 '**TLC:** Explore un informe organizado por Tratado de Libre Comercio.',
                                 '**País:** Explore un informe organizado por país.',
                                  '**Colombia:** Explore un informe organizado de Colombia.',
                                 '**Departamento:** Explore un informe organizado por departamento.'),
                                # Aclaración
                                help = "Seleccione una de las opciones para mostrar el contenido relacionado.",
                                on_change=limpiar_cache)

    # Continente
    if eleccion_usuario == "**Continente:** Explore un informe organizado por continente a nivel mundial.":
        st.markdown("""
        <div class="justify-text">
        <p>Elija un continente para descargar el informe de interés.</p>
        </div>               
        """, unsafe_allow_html=True)
        continente_elegido = st.selectbox('Seleccione un continente:', selectores.selector_continentes(sesion_activa), index=None, placeholder='Elija una opción', help = 'Aquí puede elegir el continente para descargar el informe de interés. Seleccione un único continente para refinar su búsqueda.', key = 'widget_continentes')
        # Se activa el proceso solo si el usuario elige una opción
        if continente_elegido:
            # Generar los documentos, registrar el evento de selección y obtener los resultados
            continente_elegido_tuple = tuple([continente_elegido])
            b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx = desc.generar_documentos(
                agrupacion='CONTINENTES',
                _sesion_activa=sesion_activa,
                continentes=continente_elegido_tuple,
                header_image_left=top_left_img,
                footer_image=bottom_right)
            # Botones de descarga
            if [b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx]:
                # Se generan los botones solo si hay archivos creados
                desc.botones_decarga_word_pdf_xlsx(b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx, 'CONTINENTES', sesion_activa, continente_elegido)
                                
   # HUB
    if eleccion_usuario == "**HUB:** Explore un informe organizado por HUB.":
        st.markdown("""
        <div class="justify-text">
        <p>Elija un HUB para descargar el informe de interés.</p>
        </div>               
        """, unsafe_allow_html=True)
        hub_elegido = st.selectbox('Seleccione un HUB:', selectores.selector_hubs(sesion_activa), index=None, placeholder='Elija una opción', help = 'Aquí puede elegir el HUB para descargar el informe de interés. Seleccione un único HUB para refinar su búsqueda.', key = 'widget_hubs')
        # Se activa el proceso solo si el usuario elige una opción
        if hub_elegido:
            # Generar los documentos, registrar el evento de selección y obtener los resultados
            hub_elegido_tuple = tuple([hub_elegido])
            b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx = desc.generar_documentos(
                agrupacion='HUBS',
                _sesion_activa=sesion_activa,
                hubs=hub_elegido_tuple,
                header_image_left=top_left_img,
                footer_image=bottom_right)
            # Botones de descarga
            if [b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx]:
                # Se generan los botones solo si hay archivos creados
                desc.botones_decarga_word_pdf_xlsx(b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx, 'HUBS', sesion_activa, hub_elegido)
            
    # TLCS
    if eleccion_usuario == '**TLC:** Explore un informe organizado por Tratado de Libre Comercio.':
        st.markdown("""
        <div class="justify-text">
        <p>Elija un TLC para descargar el informe de interés.</p>
        </div>               
        """, unsafe_allow_html=True)
        tlc_elegido = st.selectbox('Seleccione un TLC:', selectores.selector_tlcs(sesion_activa), index=None, placeholder='Elija una opción', help = 'Aquí puede elegir el TLC para descargar el informe de interés. Seleccione un único TLC para refinar su búsqueda.', key = 'widget_tlcs')
        # Se activa el proceso solo si el usuario elige una opción
        if tlc_elegido:
            # Generar los documentos, registrar el evento de selección y obtener los resultados
            tlc_elegido_tuple = tuple([tlc_elegido])
            b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx = desc.generar_documentos(
                agrupacion='TLCS',
                _sesion_activa=sesion_activa,
                tlcs=tlc_elegido_tuple,
                header_image_left=top_left_img,
                footer_image=bottom_right)
            # Botones de descarga
            if [b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx]:
                # Se generan los botones solo si hay archivos creados
                desc.botones_decarga_word_pdf_xlsx(b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx, 'TLCS', sesion_activa, tlc_elegido)

    # País
    if eleccion_usuario == "**País:** Explore un informe organizado por país.":
        st.markdown("""
        <div class="justify-text">
        <p>Elija un continente y luego un país para descargar el informe de interés.</p>
        </div>               
        """, unsafe_allow_html=True)
        continente_pais = st.selectbox('Seleccione un continente:', selectores.selector_continentes_paises(sesion_activa), index=None, placeholder='Elija una opción', help = 'Aquí puede elegir el continente para descargar el informe de interés. Seleccione un único continente para refinar su búsqueda.', key = 'widget_continentes_pais')
        if continente_pais:
            pais_elegido = st.selectbox('Seleccione un país:', selectores.selector_paises(sesion_activa, continente_pais), index=None, placeholder='Elija una opción', help = 'Aquí puede elegir el país para descargar el informe de interés. Seleccione un único país para refinar su búsqueda.', key = 'widget_pais')        
            # Se activa el proceso solo si el usuario elige una opción
            if pais_elegido:
                # Generar los documentos, registrar el evento de selección y obtener los resultados
                pais_elegido_tuple = tuple([pais_elegido])
                b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx = desc.generar_documentos(
                    agrupacion='PAISES',
                    _sesion_activa=sesion_activa,
                    paises=pais_elegido_tuple,
                    header_image_left=top_left_img,
                    footer_image=bottom_right)
                # Botones de descarga
                if [b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx]:
                    # Se generan los botones solo si hay archivos creados
                    desc.botones_decarga_word_pdf_xlsx(b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx, 'PAISES', sesion_activa, pais_elegido)
                    
    # Colombia 
    if eleccion_usuario =="**Colombia:** Explore un informe organizado de Colombia.":
        # Generar los documentos, registrar el evento de selección y obtener los resultados
            b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx = desc.generar_documentos(
                agrupacion='COLOMBIA',
                _sesion_activa=sesion_activa,
                header_image_left=top_left_img,
                footer_image=bottom_right)
            # Botones de descarga
            if [b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx]:
                # Se generan los botones solo si hay archivos creados
                desc.botones_decarga_word_pdf_xlsx(b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx, 'COLOMBIA', sesion_activa, 'Colombia')

    # Departamento
    if eleccion_usuario == "**Departamento:** Explore un informe organizado por departamento.":
        st.markdown("""
        <div class="justify-text">
        <p>Elija un departamento para descargar el informe de interés.</p>
        </div>               
        """, unsafe_allow_html=True)
        departamento_elegido = st.selectbox('Seleccione un departamento:', selectores.selector_departamento(sesion_activa), index=None, placeholder='Elija una opción', help = 'Aquí puede elegir el departamento para descargar el informe de interés. Seleccione un único departamento para refinar su búsqueda.', key = 'widget_departamentos')
        # Se activa el proceso solo si el usuario elige una opción
        if departamento_elegido:
            # Generar los documentos, registrar el evento de selección y obtener los resultados
            departamento_elegido_tuple = tuple([departamento_elegido])
            b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx = desc.generar_documentos(
                agrupacion='DEPARTAMENTOS',
                _sesion_activa=sesion_activa,
                departamentos=departamento_elegido_tuple,
                header_image_left=top_left_img,
                footer_image=bottom_right)
            # Botones de descarga
            if [b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx]:
                # Se generan los botones solo si hay archivos creados
                desc.botones_decarga_word_pdf_xlsx(b64_docx, b64_pdf, b64_xlsx, file_path_docx, pdf_file_path, file_path_xlsx, 'DEPARTAMENTOS', sesion_activa, departamento_elegido)

    # Footer
    st.image(image=footer, caption=None, use_column_width="always")


#########
# Fuentes
#########

def page_fuentes():
    # Banner
    st.image(image=banner, caption=None, use_column_width="always")
    # Contenido de fuentes
    st.markdown("""
    <style>
    .justify-text {
        text-align: justify;
    }
    .justify-text p {
        margin-bottom: 10px; /* Espacio entre los párrafos */
    }
    .justify-text .indent {
        margin-left: 20px; /* Ajusta este valor para cambiar la indentación del texto */
    }            
    </style>
    <h1>Documentos Tres Ejes</h1>
    <h2>Fuentes</h2>
    <div class="justify-text">
        <p><strong>1. Exportaciones - (Fuente: DANE - DIAN - Cálculos ProColombia):</strong> La información estadística de comercio exterior que produce la DIAN se genera a partir de los datos de los registros administrativos de importaciones y exportaciones (declaraciones de importación y exportación) los cuales son validados, procesados y analizados a partir de la metodología de la Organización de las Naciones Unidas (ONU), de la Comunidad Andina de Naciones (CAN) y los criterios de calidad definidos por el Departamento Administrativo Nacional de Estadística (DANE).</p>
        <p><strong>2. Inversión - (Fuente: Banco de la República - Cálculos ProColombia):</strong> La inversión directa son los aportes de capital en los que existe una relación accionaria entre el inversionista y la empresa que reside en una economía distinta. Además, el inversionista tiene una influencia significativa en la toma de decisiones de la empresa. La inversión directa es una categoría dentro de la Balanza de Pagos, y puede ser de dos formas:</p>
        <p class="indent"><strong>- Inversión extranjera directa en Colombia (IED):</strong> Es la inversión directa realizada por inversionistas residentes en el exterior en empresas residentes en Colombia. También se denomina inversión directa pasiva.</p>
        <p class="indent"><strong>- Inversión directa de Colombia en el exterior (IDCE):</strong> Es la inversión directa realizada por inversionistas residentes en Colombia en empresas residentes en el exterior. También se denomina inversión directa activa.</p>
        <p><strong>3. Turismo - (Fuente: Migración Colombia - Cálculos ProColombia):</strong> La información estadística de Migración Colombia muestran la llegada de extranjeros no residentes a Colombia por país de residencia, departamento y ciudad de hospedaje. Los datos excluyen el registro de residentes venezolanos reportado por Migración Colombia, al igual el número de colombianos residentes en el exterior o cruceristas.</p>
        <p><strong>4. Conectividad Aérea - (Fuente: OAG - Cálculos ProColombia):</strong> Contiene información detallada sobre los vuelos nacionales en Colombia, incluyendo la aerolínea, la ciudad y el departamento de origen y destino, así como las frecuencias de los vuelos registrados. También incluye datos sobre las regiones de origen y destino y la semana de análisis de la información.</p>
    </div>
    """, unsafe_allow_html=True)

    # Footer
    st.image(image=footer, caption=None, use_column_width="always")



########################################
# Mostrar contenido de todas las páginas
########################################
if __name__ == "__main__":
    main()
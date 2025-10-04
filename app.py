from functools import wraps
from imaplib import _Authenticator
from flask import Flask, flash, jsonify, make_response, render_template, redirect, request, session, url_for , send_file, abort# type: ignore
from conexion import DatabaseAuthenticator
from datetime import datetime
import io
import pandas as pd
import xlsxwriter
from fpdf import FPDF
import calendar
import os

app = Flask(__name__)
app.secret_key = 'Ale1209.'
app.config['PERMANENT_SESSION_LIFETIME'] = 3600  # 1 hora de duración de sesión
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SECURE'] = False  # Cambiar a True en producción con HTTPS
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_REFRESH_EACH_REQUEST'] = True  # Refrescar la sesión en cada solicitud

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'usuario' not in session:
            # Guardar la URL a la que el usuario intentaba acceder
            session['next_url'] = request.url
            flash('Debes iniciar sesión para acceder a esta página.', 'warning')
            return redirect(url_for('login'))
        # Refrescar la sesión para extender su duración
        session.modified = True
        return f(*args, **kwargs)
    return decorated_function



@app.route('/')
def index():
    # Redirigir al dashboard si ya está autenticado, de lo contrario al login
    if 'usuario' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    # Si ya hay una sesión activa, redirigir al dashboard
    if 'usuario' in session:
        return redirect(url_for('dashboard'))
        
    if request.method == 'POST':
        usuario = request.form.get('usuario', '').strip()
        contrasena = request.form.get('contrasena', '').strip()
        
        if not usuario or not contrasena:
            flash('Por favor ingrese usuario y contraseña', 'warning')
        else:
            db_auth = DatabaseAuthenticator()
            if db_auth.authenticate_user(usuario, contrasena):
                # Configurar la sesión
                session['usuario'] = usuario
                session['_fresh'] = True
                
                # Redirigir a la página solicitada originalmente o al dashboard
                next_page = session.pop('next_url', None)
                return redirect(next_page or url_for('dashboard'))
            else:
                flash('Usuario o contraseña incorrectos', 'danger')
    
    # Si es GET o si hubo un error en el POST, mostrar el formulario de login
    response = make_response(render_template('login.html'))
    # Prevenir caché de la página de login
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    return response


@app.route('/dashboard')
@login_required
def dashboard():
    db_auth = DatabaseAuthenticator()
    ventas_totales = float(db_auth.obtener_ventas_totales_hoy() or 0)
    litros_distribuidos = float(db_auth.obtener_litros_distribuidos_hoy() or 0)
    productos_vendidos = float(db_auth.obtener_productos_vendidos_hoy() or 0)
    ventas_por_producto = db_auth.obtener_ventas_por_producto()
    
    # Obtener inventario consolidado
    inventario_consolidado = db_auth.obtener_inventario_consolidado()
    inv_labels = [row[0] for row in inventario_consolidado]
    inv_data = [float(row[1] or 0) for row in inventario_consolidado]
    
    return render_template(
        'dashboard.html',
        ventas_totales=ventas_totales,
        litros_distribuidos=litros_distribuidos,
        productos_vendidos=productos_vendidos,
        ventas_por_producto=ventas_por_producto,  # <-- Pasamos la lista de tuplas
        inv_labels=inv_labels,
        inv_data=inv_data
    )


@app.route('/perfil')
@login_required
def perfil():
    usuario = session.get('usuario', 'Invitado')
    return render_template('perfil.html', usuario=usuario)

@app.route('/salir')
@app.route('/salir')
@app.route('/logout')
def logout():
    # Limpiar la sesión
    session.clear()
    flash('Has cerrado sesión correctamente', 'success')
    # Redirigir al login
    response = make_response(redirect(url_for('login')))
    # Prevenir caché
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

@app.route('/fuel-inventory')
@login_required
def fuel_inventory():
    db_auth = DatabaseAuthenticator()
    if request.method == 'POST':
        tipo = request.form['tipo']
        inventario_inicial = request.form['inventario_inicial']
        entrada = request.form['entrada']
        salida = request.form['salida']
        inventario_final = request.form['inventario_final']
        fecha = request.form['fecha']
        db_auth.agregar_registro_inventario(tipo, inventario_inicial, entrada, salida, inventario_final, fecha)
        flash('Registro de inventario agregado correctamente')
        return redirect(url_for('fuel_inventory'))
    registros = db_auth.obtener_todos_los_registros_inventario()
    return render_template('fuel_inventory.html', registros=registros)

@app.route('/truck-registry')
@login_required
def truck_registry():
    db_auth = DatabaseAuthenticator()
    if request.method == 'POST':
        placa = request.form['placa']
        capacidad = request.form['capacidad']
        tipo_nombre = request.form['tipo']
        tipo_id = db_auth.obtener_tipo_combustible_id(tipo_nombre)
        conductor = request.form['conductor']
        estado = request.form['estado']
        ultimo_mantenimiento = request.form['ultimo_mantenimiento']
        ubicacion_actual = request.form.get('ubicacion_actual', '')
        proximo_mantenimiento = request.form.get('proximo_mantenimiento', '')

        db_auth.agregar_pipa(
            placa, capacidad, tipo_id, conductor, estado,
            ubicacion_actual, ultimo_mantenimiento, proximo_mantenimiento
        )
        flash('Pipa agregada correctamente')
        return redirect(url_for('truck_registry'))
    pipas = db_auth.obtener_todas_las_pipas()
    return render_template('truck_registry.html', pipas=pipas)

def format_inventory_data(registros):
    for registro in registros:
        registro['InventarioInicial'] = "{:,.2f}".format(float(registro['InventarioInicial'] or 0))
        registro['Entrada'] = "{:,.2f}".format(float(registro['Entrada'] or 0))
        registro['Salida'] = "{:,.2f}".format(float(registro['Salida'] or 0))
        registro['InventarioFinal'] = "{:,.2f}".format(float(registro['InventarioFinal'] or 0))
    return registros

@app.route('/inventory-and-trucks')
@login_required
def inventory_and_trucks():
    db_auth = DatabaseAuthenticator()
    tipos_combustible = db_auth.obtener_tipos_combustible_con_id()
    pipas = db_auth.obtener_todas_las_pipas()

    # Paso 1: Obtener el tipo por defecto (ejemplo: el primero de la lista)
    tipo_default_id = tipos_combustible[0]['id'] if tipos_combustible else None
    inventario_inicial = db_auth.obtener_ultimo_inventario_final(tipo_default_id) if tipo_default_id else 0
    
    if request.method == 'POST':
        form_type = request.form.get('form_type')
        if form_type == 'inventario':
            tipo_id = int(request.form['tipo'])
            inventario_inicial = float(request.form.get('inventario_inicial') or 0)
            entrada = float(request.form.get('entrada') or 0)
            salida = float(request.form.get('salida') or 0)
            inventario_final = inventario_inicial + entrada - salida
            fecha = request.form['fecha']
            db_auth.agregar_registro_inventario(tipo_id, inventario_inicial, entrada, salida, inventario_final, fecha)
            flash('Registro de inventario agregado correctamente')
            return redirect(url_for('inventory_and_trucks'))
        elif form_type == 'pipa':
            placa = request.form['placa']
            capacidad = request.form['capacidad']
            tipo_id = int(request.form['tipo_combustible_id'])
            conductor_asignado = request.form.get('conductor_asignado')
            estado = request.form['estado']
            ubicacion_actual = request.form.get('ubicacion_actual')
            ultimo_mantenimiento = request.form.get('ultimo_mantenimiento')
            proximo_mantenimiento = request.form.get('proximo_mantenimiento')
            db_auth.agregar_pipa(
                placa, capacidad, tipo_id, conductor_asignado,
                estado, ubicacion_actual, ultimo_mantenimiento, proximo_mantenimiento
            )
            flash('Pipa agregada correctamente')
            return redirect(url_for('inventory_and_trucks'))

    # --- Filtrado por mes y paginación SIEMPRE ---
    page = int(request.args.get('page', 1))
    per_page = 5
    now = datetime.now()
    mes = int(request.args.get('mes', now.month))
    anio = int(request.args.get('anio', now.year))

    registros = db_auth.obtener_registros_inventario_mes(mes, anio)
    total = len(registros)
    start = (page - 1) * per_page
    end = start + per_page
    registros_pagina = registros[start:end]

    registros_con_nombre = [
        {
            'InventarioID': registro.InventarioID,
            'TipoCombustibleID': int(registro.TipoCombustibleID),
            'InventarioInicial': float(registro.InventarioInicial or 0),
            'Entrada': float(registro.Entrada or 0),
            'Salida': float(registro.Salida or 0),
            'InventarioFinal': float(registro.InventarioFinal or 0),
            'Fecha': registro.Fecha,
            'NombreTipo': db_auth.obtener_nombre_tipo_combustible(registro.TipoCombustibleID),
            'EsAutomatico': int(getattr(registro, 'EsAutomatico', 0) or 0)  # <-- AÑADIDO
        }
        for registro in registros_pagina
    ]

    pipas = db_auth.obtener_todas_las_pipas()

    return render_template(
        'inventory_and_trucks.html',
        registros=registros_con_nombre,
        pipas=pipas,
        tipos_combustible=tipos_combustible,
        page=page,
        total=total,
        per_page=per_page,
        mes=mes,
        anio=anio
    )
@app.route('/inventario')
@login_required
def inventario():
    db = DatabaseAuthenticator()
    registros = db.obtener_registros_inventario_completo()
    print(registros)  # <-- Aquí revisa si EsAutomatico tiene valor
    return render_template('inventory_and_trucks.html', registros=registros)

@app.route('/obtener_inventario_inicial')
@login_required
def obtener_inventario_inicial():
    tipo_id = request.args.get('tipo')  # <-- Ya es el ID
    db_auth = DatabaseAuthenticator()
    inventario_inicial = db_auth.obtener_ultimo_inventario_final(tipo_id)
    return jsonify({'inventario_inicial': float(inventario_inicial or 0)})


@app.route('/editar_registro/<int:id>', methods=['POST'])
@login_required
def editar_registro(id):
    db_auth = DatabaseAuthenticator()
    print("DEBUG tipo recibido:", request.form['tipo'])
    tipo_id = int(request.form['tipo'])  # <-- Ahora recibimos el ID
    inventario_inicial = float(request.form['inventario_inicial'])
    entrada = float(request.form.get('entrada', 0) or 0)
    salida = float(request.form.get('salida', 0) or 0)
    inventario_final = inventario_inicial + entrada - salida
    fecha = request.form['fecha']
    db_auth.actualizar_registro_inventario(
        id,
        tipo_id,           # <-- Pasa el ID aquí
        inventario_inicial,
        entrada,
        salida,
        inventario_final,
        fecha
    )
    return redirect(url_for('inventory_and_trucks'))

@app.route('/eliminar_registro/<int:id>', methods=['POST'])
@login_required
def eliminar_registro(id):
    db_auth = DatabaseAuthenticator()
    db_auth.eliminar_registro_inventario(id)
    flash('Registro eliminado correctamente')
    return redirect(url_for('inventory_and_trucks'))

@app.route('/datos_litros_distribuidos')
@login_required
def datos_litros_distribuidos():
    db_auth = DatabaseAuthenticator()
    litros_distribuidos = db_auth.obtener_litros_distribuidos_hoy()
    return jsonify({'litros_distribuidos': litros_distribuidos})

@app.route('/editar_pipa/<int:id>', methods=['POST'])
@login_required
def editar_pipa(id):
    db_auth = DatabaseAuthenticator()
    placa = request.form['placa']
    capacidad = request.form['capacidad']
    tipo_id = int(request.form['tipo_combustible_id'])  # <-- Ya es el ID
    conductor_asignado = request.form['conductor_asignado']
    estado = request.form['estado']
    ubicacion_actual = request.form['ubicacion_actual']
    ultimo_mantenimiento = request.form['ultimo_mantenimiento']
    proximo_mantenimiento = request.form['proximo_mantenimiento']
    db_auth.actualizar_pipa(id, placa, capacidad, tipo_id, conductor_asignado, estado, ubicacion_actual, ultimo_mantenimiento, proximo_mantenimiento)
    flash('Pipa actualizada correctamente')
    return redirect(url_for('inventory_and_trucks'))

@app.route('/eliminar_pipa/<int:id>', methods=['POST'])
@login_required
def eliminar_pipa(id):
    db_auth = DatabaseAuthenticator()
    db_auth.eliminar_pipa(id)
    flash('Pipa eliminada correctamente')
    return redirect(url_for('inventory_and_trucks'))

@app.route('/agregar_pipa_ajax', methods=['POST'])
@login_required
def agregar_pipa_ajax():
    try:
        db_auth = DatabaseAuthenticator()
        placa = request.form['placa']
        capacidad = request.form['capacidad']
        tipo_combustible_id = request.form['tipo_combustible_id']
        conductor_asignado = request.form.get('conductor_asignado')
        estado = request.form['estado']
        ubicacion_actual = request.form.get('ubicacion_actual')
        ultimo_mantenimiento = request.form.get('ultimo_mantenimiento')
        proximo_mantenimiento = request.form.get('proximo_mantenimiento')
        
        db_auth.agregar_pipa(
            placa, capacidad, tipo_combustible_id, conductor_asignado,
            estado, ubicacion_actual, ultimo_mantenimiento, proximo_mantenimiento
        )
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/listar_pipas_ajax')
@login_required
def listar_pipas_ajax():
    db_auth = DatabaseAuthenticator()
    pipas = db_auth.obtener_todas_las_pipas()
    tipos_combustible = db_auth.obtener_tipos_combustible_con_id()
    return render_template(
        'inventory_and_trucks.html',
        pipas=pipas,
        tipos_combustible=tipos_combustible,
    )

@app.route('/eliminar_producto/<int:id>', methods=['POST'])
@login_required
def eliminar_producto(id):
    db_auth = DatabaseAuthenticator()
    db_auth.eliminar_producto(id)
    flash('Producto eliminado correctamente')
    return redirect(url_for('product_inventory'))

@app.route('/product-inventory', methods=['GET', 'POST'])
@login_required
def product_inventory():
    db_auth = DatabaseAuthenticator()
    if request.method == 'POST' and 'guardar_producto' in request.form:
        producto_id = request.form.get('producto_id')
        codigo = request.form['codigo']
        nombre = request.form['nombre']
        precio = float(request.form['precio'])
        cantidad = int(request.form['cantidad'])
        if producto_id:  # Editar
            db_auth.actualizar_producto(producto_id, codigo, nombre, precio, cantidad)
            flash('Producto actualizado correctamente')
        else:  # Agregar
            db_auth.agregar_producto(codigo, nombre, precio, cantidad)
            flash('Producto agregado correctamente')
        return redirect(url_for('product_inventory'))

    # --- BÚSQUEDA Y ORDENACIÓN ---
    search = request.args.get('search', '').strip()
    codigo_seleccionado = request.args.get('codigo', '').strip()
    sort_cantidad = request.args.get('sort_cantidad', '')
    page = int(request.args.get('page', 1))
    per_page = 5

    # Obtener todos los productos
    productos = db_auth.obtener_todos_los_productos()
    
    # Obtener lista de códigos únicos para el desplegable
    codigos = sorted(list({p.Codigo for p in productos if p.Codigo}))
    
    # Aplicar filtros de búsqueda
    if codigo_seleccionado:
        productos = [p for p in productos if p.Codigo == codigo_seleccionado]
    
    if search:
        productos = [
            p for p in productos
            if search.lower() in p.Codigo.lower() or search.lower() in p.Nombre.lower()
        ]
    
    # Aplicar ordenación por cantidad si se solicitó
    if sort_cantidad == 'asc':
        productos.sort(key=lambda x: x.Cantidad, reverse=False)
    elif sort_cantidad == 'desc':
        productos.sort(key=lambda x: x.Cantidad, reverse=True)

    total = len(productos)
    start = (page - 1) * per_page
    end = start + per_page
    productos_pagina = productos[start:end]

    return render_template(
        'product_inventory.html',
        productos=productos_pagina,
        codigos=codigos,
        codigo_seleccionado=codigo_seleccionado,
        page=page,
        total=total,
        per_page=per_page,
        search=search,
        sort_cantidad=sort_cantidad
    )

@app.route('/ventas', methods=['GET', 'POST'])
@login_required
def ventas():
    db_auth = DatabaseAuthenticator()
    clientes = db_auth.obtener_todos_los_clientes()
    productos = db_auth.obtener_todos_los_productos()
    fecha_actual = datetime.now().strftime('%Y-%m-%d')

    # Inicializar variables de sesión si no existen
    if 'carrito' not in session:
        session['carrito'] = []
    if 'cliente_seleccionado' not in session:
        session['cliente_seleccionado'] = ''
    if 'observaciones' not in session:
        session['observaciones'] = ''
    if 'descuento' not in session:
        session['descuento'] = 0.0
    if 'metodo_pago' not in session:
        session['metodo_pago'] = 'Efectivo'

    if request.method == 'POST':
        # Limpiar todo si se presiona "Cancelar"
        if 'cancelar_venta' in request.form:
            session['carrito'] = []
            session['cliente_seleccionado'] = ''
            session['observaciones'] = ''
            session['descuento'] = 0.0
            session['metodo_pago'] = 'Efectivo'
            session.modified = True
            return redirect(url_for('ventas'))

        # Agregar producto al carrito
        if 'agregar_producto' in request.form:
            session['cliente_seleccionado'] = request.form.get('cliente', '')
            session['observaciones'] = request.form.get('observaciones', '')
            session['descuento'] = float(request.form.get('descuento', 0))
            session['metodo_pago'] = request.form.get('metodo_pago', 'Efectivo')
            producto_id = int(request.form['producto_id'])
            cantidad = int(request.form['cantidad'])
            producto = next((p for p in productos if p.ProductoID == producto_id), None)
            if producto and cantidad > 0 and cantidad <= producto.Cantidad:
                for item in session['carrito']:
                    if item['producto_id'] == producto_id:
                        item['cantidad'] += cantidad
                        break
                else:
                    session['carrito'].append({
                        'producto_id': producto_id,
                        'codigo': producto.Codigo,
                        'nombre': producto.Nombre,
                        'precio': float(producto.Precio),
                        'cantidad': cantidad
                    })
                session.modified = True
            else:
                flash('Cantidad inválida o producto sin stock')
            return redirect(url_for('ventas'))

        # Eliminar producto del carrito
        elif 'eliminar_producto' in request.form:
            producto_id = int(request.form['eliminar_producto'])
            session['carrito'] = [item for item in session['carrito'] if item['producto_id'] != producto_id]
            session.modified = True
            return redirect(url_for('ventas'))

        # Finalizar venta
        elif 'finalizar_venta' in request.form:
            cliente_id = int(session.get('cliente_seleccionado', 0) or 0)
            fecha = fecha_actual
            metodo_pago = request.form['metodo_pago']
            observaciones = request.form.get('observaciones', '')
            carrito = session.get('carrito', [])
            subtotal = sum(item['precio'] * item['cantidad'] for item in carrito)
            iva = subtotal * 0.12
            descuento = float(request.form.get('descuento', 0))
            total = subtotal + iva - descuento

            venta_id = db_auth.agregar_venta(cliente_id, fecha, subtotal, iva, descuento, total, metodo_pago, observaciones)
            for item in carrito:
                db_auth.agregar_detalle_venta(venta_id, item['producto_id'], item['cantidad'], item['precio'], item['precio'] * item['cantidad'])
                db_auth.rebajar_stock_producto(item['producto_id'], item['cantidad'])

            session['carrito'] = []
            session['cliente_seleccionado'] = ''
            session['observaciones'] = ''
            session['descuento'] = 0.0
            session['metodo_pago'] = 'Efectivo'
            flash('Venta realizada correctamente')
            return redirect(url_for('ventas'))

    # Calcular totales para mostrar en el resumen
    carrito = session.get('carrito', [])
    subtotal = sum(item['precio'] * item['cantidad'] for item in carrito)
    iva = subtotal * 0.12
    descuento = session.get('descuento', 0.0)
    total = subtotal + iva - descuento

    cliente_seleccionado = session.get('cliente_seleccionado', '')
    observaciones = session.get('observaciones', '')
    metodo_pago = session.get('metodo_pago', 'Efectivo')

    return render_template(
        'ventas.html',
        clientes=clientes,
        productos=productos,
        carrito=carrito,
        subtotal=subtotal,
        iva=iva,
        descuento=descuento,
        total=total,
        fecha_actual=fecha_actual,
        cliente_seleccionado=cliente_seleccionado,
        observaciones=observaciones,
        metodo_pago=metodo_pago
    )

@app.route('/historial_ventas')
@login_required
def historial_ventas():
    db_auth = DatabaseAuthenticator()
    # Obtener filtros de la URL
    from datetime import datetime
    now = datetime.now()
    mes = request.args.get('mes', default=now.month, type=int)
    anio = request.args.get('anio', default=now.year, type=int)
    page = request.args.get('page', default=1, type=int)
    per_page = 10

    ventas = db_auth.obtener_historial_ventas(mes=mes, anio=anio, page=page, per_page=per_page)
    total = db_auth.contar_historial_ventas(mes=mes, anio=anio)
    total_pages = (total + per_page - 1) // per_page

    return render_template(
        'ventas.html',
        active_tab='historial',
        ventas=ventas,
        mes=mes,
        anio=anio,
        page=page,
        total_pages=total_pages
    )


@app.route('/ventas_combustible', methods=['GET', 'POST'])
@login_required
def ventas_combustible():
    db = DatabaseAuthenticator()
    clientes = db.obtener_clientes_para_combustible()
    tipos_combustible = db.obtener_tipos_combustible_con_precio()
    # Filtros
    cliente_id = request.args.get('filtro_cliente')
    fecha = request.args.get('filtro_fecha')
    pagina = int(request.args.get('pagina', 1))
    tab = request.args.get('tab','nueva')
    historial, total_paginas = db.obtener_historial_ventas_combustible(cliente_id, fecha, pagina)
    return render_template(
        'ventas_combustible.html',
        clientes=clientes,
        tipos_combustible=tipos_combustible,
        historial=historial,
        total_paginas=total_paginas,
        pagina=pagina,
        filtro_cliente=cliente_id,
        filtro_fecha=fecha,
        tab = tab
    )

@app.route('/registrar_venta_combustible', methods=['POST'])
@login_required
def registrar_venta_combustible():
    db = DatabaseAuthenticator()
    # Obtén los datos del formulario
    cliente_id = request.form.get('cliente_id')
    fecha = request.form.get('fecha')
    metodo_pago = request.form.get('metodo_pago')
    observaciones = request.form.get('observaciones')
    detalles_json = request.form.get('detalles')
    try:
        import json
        detalles = json.loads(detalles_json)
    except Exception:
        detalles = []
    exito = db.registrar_venta_combustible(cliente_id, fecha, metodo_pago, observaciones, detalles)
    if exito:
        flash('Venta registrada con éxito.', 'success')
    else:
        flash('Error al registrar la venta.', 'danger')
    return redirect(url_for('ventas_combustible', tab='nueva'))


@app.route('/estadisticas_combustible', methods=['GET'])
@login_required
def estadisticas_combustible():
    db_auth = DatabaseAuthenticator()
    
    # Obtener parámetros de filtro para ventas de combustible
    cliente_combustible_id = request.args.get('cliente_combustible')
    mes_combustible = request.args.get('mes_combustible')
    anio_combustible = request.args.get('anio_combustible')

    # Obtener parámetros de filtro para productos más vendidos
    cliente_producto_id = request.args.get('cliente_producto')
    mes_producto = request.args.get('mes_producto')
    anio_producto = request.args.get('anio_producto')

    try:
        # Obtener los datos crudos de la base de datos
        ventas_raw = db_auth.obtener_ventas_mensuales_combustible_agrupadas(
            cliente_id=cliente_combustible_id,
            mes=mes_combustible,
            anio=anio_combustible
        )
        
        productos_raw = db_auth.obtener_productos_mas_vendidos(
            cliente_id=cliente_producto_id,
            mes=mes_producto,
            anio=anio_producto
        )

        # Formatear los datos para las gráficas
        ventas_mensuales_combustible = []
        for row in ventas_raw:
            # Asegurarse de que cada valor tenga el tipo correcto
            mes = int(row[0])
            tipo_combustible = str(row[1])
            total_litros = float(row[2]) if row[2] is not None else 0.0
            ventas_mensuales_combustible.append([mes, tipo_combustible, total_litros])
        
        # Ordenar por mes para asegurar el orden correcto en la gráfica
        ventas_mensuales_combustible.sort(key=lambda x: x[0])
        
        # Formatear productos más vendidos
        productos_mas_vendidos = []
        for row in productos_raw:
            nombre_producto = str(row[0])
            cantidad = int(row[1]) if row[1] is not None else 0
            productos_mas_vendidos.append([nombre_producto, cantidad])

    except Exception as e:
        print(f"Error al obtener datos para gráficas: {str(e)}")
        flash(f"Ocurrió un error al obtener los datos: {e}", "danger")
        ventas_mensuales_combustible = []
        productos_mas_vendidos = []

    # Obtener lista de clientes para los filtros
    clientes = db_auth.obtener_todos_los_clientes()

    # Debug: Imprimir datos que se enviarán a la plantilla
    print("Datos de ventas mensuales:", ventas_mensuales_combustible)
    print("Productos más vendidos:", productos_mas_vendidos)

    return render_template(
        'estadisticas_combustible.html',
        ventas_mensuales_combustible=ventas_mensuales_combustible,
        productos_mas_vendidos=productos_mas_vendidos,
        clientes=clientes
    )

@app.route('/productos_mas_vendidos', methods=['GET'])
@login_required
def productos_mas_vendidos():
    db_auth = DatabaseAuthenticator()
    clientes = db_auth.obtener_todos_los_clientes()
    cliente_id = request.args.get('cliente_id')
    dia = request.args.get('dia')
    mes = request.args.get('mes')
    anio = request.args.get('anio')
    productos = db_auth.obtener_productos_mas_vendidos_filtrado(cliente_id, dia, mes, anio)
    return render_template(
        'productos_mas_vendidos.html',
        productos=productos,
        clientes=clientes,
        cliente_id=cliente_id,
        dia=dia,
        mes=mes,
        anio=anio
    )

@app.route('/descargar_reporte/<reporte>/<formato>')
@login_required
def descargar_reporte(reporte, formato):
    fecha_inicio = request.args.get('fecha_inicio')
    fecha_fin = request.args.get('fecha_fin')
    db_auth = DatabaseAuthenticator()

    if not fecha_inicio or not fecha_fin:
        abort(400, "Debe seleccionar un rango de fechas.")

    # Obtén los datos reales según el reporte
    if reporte == 'inventario_combustible':
        data = db_auth.obtener_inventario_combustible(fecha_inicio, fecha_fin)
        columns = ['Fecha', 'Combustible', 'Entrada', 'Salida', 'Saldo']
        nombre = "Inventario_Combustible"
    elif reporte == 'ventas_combustible':
        data = db_auth.obtener_ventas_combustible(fecha_inicio, fecha_fin)
        columns = ['Fecha', 'Cliente', 'Combustible', 'Litros']
        nombre = "Ventas_Combustible"
    elif reporte == 'ventas_productos':
        data = db_auth.obtener_ventas_productos(fecha_inicio, fecha_fin)
        columns = ['Fecha', 'Producto', 'Cantidad', 'Total']
        nombre = "Ventas_Productos"
    elif reporte == 'inventario_productos':
        data = db_auth.obtener_inventario_productos(fecha_inicio, fecha_fin)
        columns = ['Producto', 'Saldo']
        nombre = "Inventario_Productos"
    else:
        abort(404, "Reporte no encontrado.")

    # Si no hay datos, puedes devolver un archivo vacío o un mensaje
    if not data:
        data = [{col: '' for col in columns}]

    # Generar Excel
    if formato == 'excel':
        df = pd.DataFrame(data, columns=columns)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Reporte')
            # Personalización: encabezado en negrita y color
            workbook = writer.book
            worksheet = writer.sheets['Reporte']
            header_format = workbook.add_format({'bold': True, 'bg_color': '#DDEEFF'})
            worksheet.set_row(0, None, header_format)
            worksheet.autofilter(0, 0, len(df), len(columns)-1)
        output.seek(0)
        filename = f"{nombre}_{fecha_inicio}_a_{fecha_fin}.xlsx"
        return send_file(output, download_name=filename, as_attachment=True, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    # Generar PDF
    elif formato == 'pdf':
        pdf = FPDF(orientation='L', unit='mm', format='A4')
        pdf.add_page()
        pdf.set_font("Arial", 'B', 14)
        pdf.cell(0, 10, f"{nombre.replace('_', ' ')}", ln=True, align='C')
        pdf.set_font("Arial", '', 12)
        pdf.cell(0, 10, f"Rango: {fecha_inicio} a {fecha_fin}", ln=True, align='C')
        pdf.ln(5)
        # Encabezados
        pdf.set_font("Arial", 'B', 11)
        col_width = 277 / len(columns)
        for col in columns:
            pdf.cell(col_width, 10, col, border=1, align='C')
        pdf.ln()
        # Filas
        pdf.set_font("Arial", '', 10)
        for row in data:
            for col in columns:
                pdf.cell(col_width, 10, str(row.get(col, '')), border=1, align='C')
            pdf.ln()
        output = io.BytesIO(pdf.output(dest='S').encode('latin1'))
        filename = f"{nombre}_{fecha_inicio}_a_{fecha_fin}.pdf"
        return send_file(output, download_name=filename, as_attachment=True, mimetype='application/pdf')

    else:
        abort(400, "Formato no soportado.")

@app.route('/maintenance')
@login_required
def maintenance():
    return render_template('maintenance.html')

@app.route('/reports')
@login_required
def reports():
    return render_template('reports.html')

if __name__ == '__main__':
    import os
    import webbrowser
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        webbrowser.open('http://127.0.0.1:5000')
    app.run(debug=True)
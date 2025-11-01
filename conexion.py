import os
import pyodbc
import datetime
import getpass  # Módulo para ocultar la contraseña al escribir
from werkzeug.security import check_password_hash, generate_password_hash

class DatabaseAuthenticator:
    def actualizar_cascada_inventario(self, inventario_id, tipo_id, nuevo_inventario_final, fecha):
        """
        Actualiza en cascada los registros posteriores al registro editado para mantener la coherencia de saldos.
        """
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            # Seleccionar todos los registros posteriores (por fecha y tipo) ordenados
            cursor.execute("""
                SELECT InventarioID, Entrada, Salida, Fecha
                FROM InventarioCombustible
                WHERE TipoCombustibleID = ? AND (Fecha > ? OR (Fecha = ? AND InventarioID > ?))
                ORDER BY Fecha ASC, InventarioID ASC
            """, (tipo_id, fecha, fecha, inventario_id))
            registros = cursor.fetchall()
            inventario_inicial = nuevo_inventario_final
            for reg in registros:
                reg_id = reg[0]
                entrada = float(reg[1] or 0)
                salida = float(reg[2] or 0)
                # Calcular nuevo inventario final
                inventario_final = inventario_inicial + entrada - salida
                # Actualizar registro
                cursor.execute("""
                    UPDATE InventarioCombustible
                    SET InventarioInicial = ?, InventarioFinal = ?
                    WHERE InventarioID = ?
                """, (inventario_inicial, inventario_final, reg_id))
                inventario_inicial = inventario_final
            self.connection.commit()
        except Exception as e:
            print("Error en actualización en cascada de inventario:", e)
        finally:
            if self.connection:
                self.connection.close()
    def obtener_saldos_actuales_todos(self):
        """
        Devuelve un diccionario con el saldo actual de todos los tipos de combustible.
        """
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT TC.Nombre, ISNULL(SUM(IC.Entrada),0) AS TotalEntradas, ISNULL(SUM(IC.Salida),0) AS TotalSalidas
                FROM InventarioCombustible IC
                JOIN TiposCombustible TC ON IC.TipoCombustibleID = TC.TipoCombustibleID
                GROUP BY TC.Nombre
            """)
            saldos = {}
            for row in cursor.fetchall():
                nombre = row[0]
                entradas = float(row[1])
                salidas = float(row[2])
                saldos[nombre] = entradas - salidas
            return saldos
        except Exception as e:
            print("Error al obtener saldos actuales de todos los combustibles:", e)
            return {}
        finally:
            if self.connection:
                self.connection.close()
    def obtener_saldo_actual(self, tipo_id):
        """
        Devuelve el saldo actual (SUM(Entrada) - SUM(Salida)) para el tipo de combustible.
        """
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT ISNULL(SUM(Entrada),0) AS TotalEntradas, ISNULL(SUM(Salida),0) AS TotalSalidas
                FROM InventarioCombustible
                WHERE TipoCombustibleID = ?
            """, (tipo_id,))
            row = cursor.fetchone()
            entradas = float(row[0]) if row and row[0] is not None else 0.0
            salidas = float(row[1]) if row and row[1] is not None else 0.0
            return entradas - salidas
        except Exception as e:
            print("Error al calcular saldo actual:", e)
            return 0.0
        finally:
            if self.connection:
                self.connection.close()

    def obtener_inventario_actual(self, tipo_id):
        """
        Devuelve el saldo real actual para el tipo de combustible,
        sumando entradas y restando salidas posteriores al último registro de inventario.
        """
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            # 1. Obtener el último registro de inventario para ese tipo
            cursor.execute("""
                SELECT TOP 1 InventarioFinal, Fecha
                FROM InventarioCombustible
                WHERE TipoCombustibleID = ?
                ORDER BY Fecha DESC, InventarioID DESC
            """, (tipo_id,))
            row = cursor.fetchone()
            saldo = float(row[0]) if row and row[0] is not None else 0.0
            fecha_ultimo = row[1] if row and row[1] is not None else None

            # 2. Sumar entradas y restar salidas posteriores a esa fecha
            if fecha_ultimo:
                cursor.execute("""
                    SELECT ISNULL(SUM(Entrada),0), ISNULL(SUM(Salida),0)
                    FROM InventarioCombustible
                    WHERE TipoCombustibleID = ? AND Fecha > ?
                """, (tipo_id, fecha_ultimo))
                movs = cursor.fetchone()
                entradas = float(movs[0]) if movs and movs[0] is not None else 0.0
                salidas = float(movs[1]) if movs and movs[1] is not None else 0.0
                saldo += entradas - salidas
            return saldo
        except Exception as e:
            print("Error al calcular inventario actual:", e)
            return 0.0
        finally:
            if self.connection:
                self.connection.close()
    def __init__(self):
        # Configuración de la conexión (usa variables de entorno con fallback)
        self.server = os.getenv('DB_SERVER', r'LAPTOP-1MHEEMP6\SQLSERVER2022')
        self.database = os.getenv('DB_NAME', 'SistemaGasolinera')
        self.username = os.getenv('DB_USER', 'sa')
        self.password = os.getenv('DB_PASSWORD', 'Ale1209.')
        self.connection = None

    def _get_connection_string(self):
        """Genera la cadena de conexión"""
        return (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={self.server};'
            f'DATABASE={self.database};'
            f'UID={self.username};'
            f'PWD={self.password}'
        )

    def authenticate_user(self, username: str, password: str):
        """Autentica un usuario y devuelve dict con datos o None.
        Compatibilidad: acepta contraseñas en texto plano o con hash.
        """
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            # Intentar traer hash (o texto) y rol; si la columna Rol no existe, hacer fallback
            try:
                cursor.execute(
                    "SELECT UsuarioID, Contrasena, ISNULL(Rol, 'encargado') as Rol FROM Usuarios WHERE NombreUsuario = ?",
                    (username,)
                )
            except pyodbc.ProgrammingError:
                cursor.execute(
                    "SELECT UsuarioID, Contrasena FROM Usuarios WHERE NombreUsuario = ?",
                    (username,)
                )
            row = cursor.fetchone()
            if not row:
                return None
            # Cuando no exista Rol, asignar 'encargado' por defecto
            user_id, stored_pw = row[0], row[1]
            role = row[2] if len(row) >= 3 else 'encargado'

            # Si parece hash (ej. pbkdf2:, scrypt:), usar check_password_hash; si no, comparar en texto plano
            if isinstance(stored_pw, str) and (':' in stored_pw):
                ok = check_password_hash(stored_pw, password)
            else:
                ok = (stored_pw == password)

            if not ok:
                return None

            return {"id": int(user_id), "usuario": username, "rol": role}
        except pyodbc.Error as ex:
            error_msg = ex.args[1] if len(ex.args) > 1 else str(ex)
            print(f"\nError de base de datos: {error_msg}")
            return None
        finally:
            if self.connection:
                self.connection.close()

    def set_user_password(self, username: str, password: str) -> bool:
        """Actualiza la contraseña del usuario con un hash seguro."""
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            pw_hash = generate_password_hash(password)
            cursor.execute("UPDATE Usuarios SET Contrasena = ? WHERE NombreUsuario = ?", (pw_hash, username))
            self.connection.commit()
            return cursor.rowcount > 0
        except Exception as e:
            print("Error al actualizar contraseña:", e)
            if self.connection:
                self.connection.rollback()
            return False
        finally:
            if self.connection:
                self.connection.close()

    def crear_usuario(self, username: str, correo: str, password: str, rol: str = 'encargado') -> bool:
        """Crea un usuario con contraseña hasheada."""
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            pw_hash = generate_password_hash(password)
            cursor.execute(
                "INSERT INTO Usuarios (NombreUsuario, Contrasena, CorreoElectronico, FechaCreacion, Rol) VALUES (?, ?, ?, GETDATE(), ?)",
                (username, pw_hash, correo, rol)
            )
            self.connection.commit()
            return True
        except Exception as e:
            print("Error al crear usuario:", e)
            if self.connection:
                self.connection.rollback()
            return False
        finally:
            if self.connection:
                self.connection.close()
    
    def obtener_ventas_mensuales_combustible_agrupadas(self, cliente_id=None, mes=None, anio=None):
        """
        Obtiene las ventas de combustible agrupadas, incluyendo el año.
        Si no hay filtros, muestra todos los datos históricos.
        """
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            params = []
            query_base = (
                "SELECT MONTH(VC.Fecha) as mes, "
                "TC.Nombre as tipo_combustible, "
                "SUM(DVC.CantidadLitros) as total_litros, "
                "YEAR(VC.Fecha) as anio "
                "FROM VentaCombustible VC "
                "JOIN DetalleVentaCombustible DVC ON VC.VentaCombustibleID = DVC.VentaCombustibleID "
                "JOIN TiposCombustible TC ON DVC.TipoCombustibleID = TC.TipoCombustibleID "
            )
            where_clauses = []
            if cliente_id:
                where_clauses.append("VC.ClienteID = ?")
                params.append(cliente_id)
            if mes:
                where_clauses.append("MONTH(VC.Fecha) = ?")
                params.append(mes)
            if anio:
                where_clauses.append("YEAR(VC.Fecha) = ?")
                params.append(anio)
            query = query_base
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            query += " GROUP BY YEAR(VC.Fecha), MONTH(VC.Fecha), TC.Nombre ORDER BY anio, mes, tipo_combustible;"
            cursor.execute(query, params)
            resultados = cursor.fetchall()
            print('DEBUG ventas mensuales:', resultados)  # Depuración
            return resultados
        except Exception as e:
            print(f"Error al obtener ventas mensuales de combustible: {e}")
            return []
        finally:
            if self.connection:
                self.connection.close()

    def obtener_productos_mas_vendidos(self, cliente_id=None, mes=None, anio=None):
        """
        Obtiene los productos más vendidos, incluyendo el año.
        Si no hay filtros, muestra el top 10 histórico.
        """
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            params = []
            query_base = (
                "SELECT TOP 10 P.Nombre, "
                "SUM(DV.Cantidad) as total_vendido, "
                "YEAR(V.Fecha) as anio "
                "FROM Ventas V "
                "JOIN DetalleVenta DV ON V.VentaID = DV.VentaID "
                "JOIN Productos P ON DV.ProductoID = P.ProductoID "
            )
            where_clauses = []
            if cliente_id:
                where_clauses.append("V.ClienteID = ?")
                params.append(cliente_id)
            if mes:
                where_clauses.append("MONTH(V.Fecha) = ?")
                params.append(mes)
            if anio:
                where_clauses.append("YEAR(V.Fecha) = ?")
                params.append(anio)
            query = query_base
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            query += " GROUP BY YEAR(V.Fecha), P.Nombre ORDER BY anio DESC, total_vendido DESC;"
            cursor.execute(query, params)
            resultados = cursor.fetchall()
            print('DEBUG productos más vendidos:', resultados)  # Depuración
            return resultados
        except Exception as e:
            print(f"Error al obtener productos más vendidos: {e}")
            return []
        finally:
            if self.connection:
                self.connection.close()
    def obtener_ventas_totales_hoy(self):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = """
                SELECT SUM(Total)
                FROM Ventas
                WHERE CAST(Fecha AS DATE) = CAST(GETDATE() AS DATE)
            """
            cursor.execute(query)
            result = cursor.fetchone()
            return float(result[0]) if result and result[0] else 0.0
        except Exception as e:
            print("Error al obtener ventas totales:", e)
            return 0.0
        finally:
            if self.connection:
                self.connection.close()
    
    def obtener_inventario_actual(self):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = """
                SELECT TC.Nombre, 
                    COALESCE(IC.InventarioFinal, 0) as Inventario
                FROM TiposCombustible TC
                LEFT JOIN (
                    SELECT TipoCombustibleID, InventarioFinal
                    FROM InventarioCombustible
                    WHERE Fecha = (
                        SELECT MAX(Fecha)
                        FROM InventarioCombustible IC2
                        WHERE IC2.TipoCombustibleID = InventarioCombustible.TipoCombustibleID
                    )
                ) IC ON TC.TipoCombustibleID = IC.TipoCombustibleID
            """
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print("Error al obtener inventario actual:", e)
            return []
        finally:
            if self.connection:
                self.connection.close()

    def obtener_inventario_consolidado(self):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = """
                SELECT TC.Nombre, 
                    SUM(IC.Entrada) - SUM(IC.Salida) as InventarioFinal
                FROM TiposCombustible TC
                LEFT JOIN InventarioCombustible IC ON TC.TipoCombustibleID = IC.TipoCombustibleID
                GROUP BY TC.Nombre
            """
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print("Error al obtener inventario consolidado:", e)
            return []
        finally:
            if self.connection:
                self.connection.close()

    def obtener_litros_distribuidos_hoy(self):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = """
                SELECT ISNULL(SUM(Salida), 0)
                FROM InventarioCombustible
                WHERE CAST(Fecha AS DATE) = CAST(GETDATE() AS DATE)
            """
            cursor.execute(query)
            result = cursor.fetchone()
            return float(result[0]) if result and result[0] else 0.0
        except Exception as e:
            print("Error al obtener litros distribuidos:", e)
            return 0.0
        finally:
            if self.connection:
                self.connection.close()

    
    
    def obtener_todos_los_registros_inventario(self):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = "SELECT * FROM InventarioCombustible"
            cursor.execute(query)
            registros = cursor.fetchall()
            return registros
        except Exception as e:
            print("Error al obtener registros de inventario:", e)
            return []
        finally:
            if self.connection:
                self.connection.close()

    def actualizar_registro_inventario(self, id, tipo_id, inventario_inicial, entrada, salida, inventario_final, fecha):
        try:
            # Establecer conexión
            with pyodbc.connect(self._get_connection_string()) as connection:
                cursor = connection.cursor()
                # Verificar si el registro es automático
                cursor.execute("SELECT EsAutomatico FROM InventarioCombustible WHERE InventarioID = ?", (id,))
                row = cursor.fetchone()
                if not row:
                    print("Error: InventarioID no encontrado.")
                    return False
                if row[0] == 1:
                    print("No se puede editar un registro de inventario generado automáticamente por una venta.")
                    return False

                # Realizar la actualización
                query = """
                    UPDATE InventarioCombustible
                    SET TipoCombustibleID = ?, InventarioInicial = ?, Entrada = ?, Salida = ?, InventarioFinal = ?, Fecha = ?
                    WHERE InventarioID = ?
                """
                cursor.execute(query, (tipo_id, inventario_inicial, entrada, salida, inventario_final, fecha, id))
                connection.commit()
                return True
        except pyodbc.Error as e:
            print("Error al actualizar registro de inventario:", e)
            return False

    def obtener_nombre_tipo_combustible(self, tipo_id):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = "SELECT Nombre FROM TiposCombustible WHERE TipoCombustibleID = ?"
            cursor.execute(query, (tipo_id,))
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            print("Error al obtener nombre del tipo de combustible:", e)
            return None
        finally:
            if self.connection:
                self.connection.close()

    def obtener_ultimo_inventario_final(self, tipo_id, cursor=None):
        close_connection = False
        if cursor is None:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            close_connection = True
        try:
            query = """
                SELECT TOP 1 InventarioFinal
                FROM InventarioCombustible
                WHERE TipoCombustibleID = ?
                ORDER BY Fecha DESC, InventarioID DESC
            """
            cursor.execute(query, (tipo_id,))
            result = cursor.fetchone()
            return float(result[0]) if result and result[0] else 0.0
        except Exception as e:
            print("Error al obtener el último inventario final:", e)
            return 0.0
        finally:
            if close_connection and self.connection:
                self.connection.close()

                
    def agregar_registro_inventario(self, tipo_id, inventario_inicial, entrada, salida, inventario_final, fecha, es_automatico=0, cursor=None):
        close_connection = False
        if cursor is None:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            close_connection = True
        try:
            # Validación básica de datos
            if not tipo_id or not fecha:
                print("Error: tipo_id o fecha vacíos")
                return False
            tipo_id = int(tipo_id)
            inventario_inicial = float(inventario_inicial)
            entrada = float(entrada)
            salida = float(salida)
            inventario_final = float(inventario_final)
            # Validar formato de fecha (YYYY-MM-DD)
            if len(fecha) != 10 or fecha[4] != '-' or fecha[7] != '-':
                print(f"Error: formato de fecha inválido: {fecha}")
                return False

            query = """
                INSERT INTO InventarioCombustible
                (TipoCombustibleID, InventarioInicial, Entrada, Salida, InventarioFinal, Fecha, EsAutomatico)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(query, tipo_id, inventario_inicial, entrada, salida, inventario_final, fecha, es_automatico)
            if close_connection:
                self.connection.commit()
                print("Registro de inventario guardado correctamente.")
            return True
        except Exception as e:
            print("Error al agregar registro de inventario:", repr(e))
            return False
        finally:
            if close_connection and self.connection:
                self.connection.close()
        
    def obtener_tipos_combustible(self):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = "SELECT Nombre FROM TiposCombustible"
            cursor.execute(query)
            tipos_combustible = [row[0] for row in cursor.fetchall()]
            return tipos_combustible
        except Exception as e:
            print("Error al obtener tipos de combustible:", e)
            return []
        finally:
            if self.connection:
                self.connection.close()

    def obtener_inventario_inicial(self, tipo_id):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = """
                SELECT InventarioInicial
                FROM InventarioCombustible
                WHERE TipoCombustibleID = ?
                ORDER BY Fecha DESC
                """
            cursor.execute(query, (tipo_id,))
            result = cursor.fetchone()
            return float(result[0]) if result and result[0] else 0.0
        except Exception as e:
            print("Error al obtener inventario inicial:", e)
            return 0.0
        finally:
            if self.connection:
                self.connection.close()

    def obtener_detalles_registro_inventario(self, id):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = """
                SELECT TipoCombustibleID, InventarioInicial, Entrada, Salida, InventarioFinal, Fecha
                FROM InventarioCombustible
                WHERE InventarioID = ?
            """
            cursor.execute(query, (id,))
            result = cursor.fetchone()
            if result:
                return {
                    'tipo_combustible_id': result[0],
                    'inventario_inicial': result[1],
                    'entrada': result[2],
                    'salida': result[3],
                    'inventario_final': result[4],
                    'fecha': result[5]
                }
            else:
                print("Error: InventarioID no encontrado.")
                return None
        except Exception as e:
            print("Error al obtener detalles del registro de inventario:", e)
            return None
        finally:
            if self.connection:
                self.connection.close()

    def eliminar_registro_inventario(self, id):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            # Verificar si el registro es automático
            cursor.execute("SELECT EsAutomatico FROM InventarioCombustible WHERE InventarioID = ?", (id,))
            row = cursor.fetchone()
            if not row:
                print("Error: InventarioID no encontrado.")
                return False
            if row[0] == 1:
                print("No se puede eliminar un registro de inventario generado automáticamente por una venta.")
                return False

            query = "DELETE FROM InventarioCombustible WHERE InventarioID = ?"
            cursor.execute(query, (id,))
            self.connection.commit()
            return True
        except Exception as e:
            print("Error al eliminar registro de inventario:", e)
            return False
        finally:
            if self.connection:
                self.connection.close()

    def obtener_tipo_combustible_id(self, tipo_nombre):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = "SELECT TipoCombustibleID FROM TiposCombustible WHERE Nombre = ?"
            cursor.execute(query, (tipo_nombre,))
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            print("Error al obtener TipoCombustibleID:", e)
            return None
        finally:
            if self.connection:
                self.connection.close()

    def obtener_registros_inventario_completo(self):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = """
                SELECT IC.InventarioID, TC.Nombre AS NombreTipo, IC.TipoCombustibleID,
                    IC.InventarioInicial, IC.Entrada, IC.Salida, IC.InventarioFinal,
                    IC.Fecha, IC.EsAutomatico
                FROM InventarioCombustible IC
                JOIN TiposCombustible TC ON IC.TipoCombustibleID = TC.TipoCombustibleID
                ORDER BY IC.Fecha DESC, IC.InventarioID DESC
            """
            cursor.execute(query)
            registros = []
            for row in cursor.fetchall():
                registros.append({
                    'InventarioID': row[0],
                    'NombreTipo': row[1],
                    'TipoCombustibleID': row[2],
                    'InventarioInicial': row[3],
                    'Entrada': row[4],
                    'Salida': row[5],
                    'InventarioFinal': row[6],
                    'Fecha': row[7],
                    'EsAutomatico': int(row[8]) if row[8] is not None else 0
                })
            return registros
        except Exception as e:
            print("Error al obtener registros de inventario:", e)
            return []
        finally:
            if self.connection:
                self.connection.close()

    def agregar_pipa(self, placa, capacidad, tipo_combustible_id, conductor_asignado, estado, ubicacion_actual, ultimo_mantenimiento, proximo_mantenimiento):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO Pipas (Placa, Capacidad, TipoCombustibleID, ConductorAsignado, Estado, UbicacionActual, UltimoMantenimiento, ProximoMantenimiento)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (placa, capacidad, tipo_combustible_id, conductor_asignado, estado, ubicacion_actual, ultimo_mantenimiento, proximo_mantenimiento))
        self.connection.commit()
        cursor.close()
        self.connection.close()

    def obtener_tipos_combustible_con_id(self):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = "SELECT TipoCombustibleID, Nombre FROM TiposCombustible"
            cursor.execute(query)
            tipos = [{'id': row[0], 'nombre': row[1]} for row in cursor.fetchall()]
            return tipos
        except Exception as e:
            print("Error al obtener tipos de combustible:", e)
            return []
        finally:
            if self.connection:
                self.connection.close()

    def actualizar_pipa(self, id, placa, capacidad, tipo_combustible_id, conductor_asignado, estado, ubicacion_actual, ultimo_mantenimiento, proximo_mantenimiento):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        cursor.execute("""
            UPDATE Pipas
            SET Placa = ?, Capacidad = ?, TipoCombustibleID = ?, ConductorAsignado = ?, Estado = ?, UbicacionActual = ?, UltimoMantenimiento = ?, ProximoMantenimiento = ?
            WHERE PipaID = ?
        """, (placa, capacidad, tipo_combustible_id, conductor_asignado, estado, ubicacion_actual, ultimo_mantenimiento, proximo_mantenimiento, id))
        self.connection.commit()
        cursor.close()
        self.connection.close()

    def eliminar_pipa(self, id):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM pipas WHERE pipaid = ?", (id,))
        self.connection.commit()
        cursor.close()
        self.connection.close()

    def obtener_todas_las_pipas(self):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = """
                SELECT P.PipaID, P.Placa, P.Capacidad, TC.Nombre AS TipoCombustible, P.ConductorAsignado, P.Estado, P.UbicacionActual, P.UltimoMantenimiento, P.ProximoMantenimiento
                FROM Pipas P
                LEFT JOIN TiposCombustible TC ON P.TipoCombustibleID = TC.TipoCombustibleID
                ORDER BY P.PipaID
            """
            cursor.execute(query)
            pipas = []
            for row in cursor.fetchall():
                pipas.append({
                    'id': row[0],
                    'placa': row[1],
                    'capacidad': row[2],
                    'tipo_combustible': row[3],  # Ahora es el nombre, no el ID
                    'conductor_asignado': row[4],
                    'estado': row[5],
                    'ubicacion_actual': row[6],
                    'ultimo_mantenimiento': row[7],
                    'proximo_mantenimiento': row[8]
                })
            return pipas
        except Exception as e:
            print("Error al obtener pipas:", e)
            return []
        finally:
            if self.connection:
                self.connection.close()

    def obtener_registros_inventario_mes(self, mes, anio):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = """
                SELECT * FROM InventarioCombustible
                WHERE MONTH(Fecha) = ? AND YEAR(Fecha) = ?
                ORDER BY Fecha DESC, InventarioID DESC
            """
            cursor.execute(query, (mes, anio))
            registros = cursor.fetchall()
            return registros
        except Exception as e:
            print("Error al obtener registros de inventario del mes:", e)
            return []
        finally:
            if self.connection:
                self.connection.close()


    def agregar_producto(self, codigo, nombre, precio, cantidad):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = "INSERT INTO Productos (Codigo, Nombre, Precio, Cantidad) VALUES (?, ?, ?, ?)"
            cursor.execute(query, (codigo, nombre, precio, cantidad))
            self.connection.commit()
        except Exception as e:
            print("Error al agregar producto:", e)
        finally:
            if self.connection:
                self.connection.close()

    def actualizar_producto(self, producto_id, codigo, nombre, precio, cantidad):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = "UPDATE Productos SET Codigo = ?, Nombre = ?, Precio = ?, Cantidad = ? WHERE ProductoID = ?"
            cursor.execute(query, (codigo, nombre, precio, cantidad, producto_id))
            self.connection.commit()
        except Exception as e:
            print("Error al actualizar producto:", e)
        finally:
            if self.connection:
                self.connection.close()

    def obtener_detalle_venta(self, venta_id):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        query = """
            SELECT p.Codigo, p.Nombre, dv.Precio, dv.Cantidad, dv.Subtotal
            FROM DetalleVenta dv
            JOIN Productos p ON dv.ProductoID = p.ProductoID
            WHERE dv.VentaID = ?
        """
        cursor.execute(query, (venta_id,))
        detalles = cursor.fetchall()
        self.connection.close()
        return detalles

    def obtener_todos_los_clientes(self):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        cursor.execute("SELECT ClienteID, Nombre FROM Clientes")
        clientes = []
        for row in cursor.fetchall():
            clientes.append(type('Cliente', (), {
                'ClienteID': row[0],
                'Nombre': row[1]
            })())
        self.connection.close()
        return clientes

    def obtener_todos_los_productos(self):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        cursor.execute("SELECT ProductoID, Codigo, Nombre, Precio, Cantidad FROM Productos")
        productos = []
        for row in cursor.fetchall():
            productos.append(type('Producto', (), {
                'ProductoID': row[0],
                'Codigo': row[1],
                'Nombre': row[2],
                'Precio': row[3],
                'Cantidad': row[4]
            })())
        self.connection.close()
        return productos
        
    def eliminar_producto(self, producto_id):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            # Verificar si el producto existe
            cursor.execute("SELECT COUNT(*) FROM Productos WHERE ProductoID = ?", (producto_id,))
            if cursor.fetchone()[0] == 0:
                return False
                
            # Eliminar el producto
            cursor.execute("DELETE FROM Productos WHERE ProductoID = ?", (producto_id,))
            self.connection.commit()
            return True
        except Exception as e:
            print(f"Error al eliminar producto: {str(e)}")
            if hasattr(self, 'connection') and self.connection:
                self.connection.rollback()
            return False
        finally:
            if hasattr(self, 'connection') and self.connection:
                self.connection.close()

    def agregar_venta(self, cliente_id, fecha, subtotal, iva, descuento, total, metodo_pago, observaciones):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO Ventas (ClienteID, Fecha, Subtotal, IVA, Descuento, Total, MetodoPago, Observaciones)
            OUTPUT INSERTED.VentaID
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (cliente_id, fecha, subtotal, iva, descuento, total, metodo_pago, observaciones))
        result = cursor.fetchone()
        venta_id = result[0] if result else None
        self.connection.commit()
        cursor.close()
        self.connection.close()
        if venta_id is None:
            raise Exception("No se pudo obtener el ID de la venta recién insertada.")
        return int(venta_id)

    def agregar_detalle_venta(self, venta_id, producto_id, cantidad, precio, subtotal):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO DetalleVenta (VentaID, ProductoID, Cantidad, Precio, Subtotal)
            VALUES (?, ?, ?, ?, ?)
        """, (venta_id, producto_id, cantidad, precio, subtotal))
        self.connection.commit()
        self.connection.close()

    def rebajar_stock_producto(self, producto_id, cantidad):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        cursor.execute("""
            UPDATE Productos SET Cantidad = Cantidad - ? WHERE ProductoID = ?
        """, (cantidad, producto_id))
        self.connection.commit()
        self.connection.close()

    def obtener_historial_ventas(self, mes=None, anio=None, page=1, per_page=10):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            params = []
            where_clauses = []
            if mes:
                where_clauses.append("MONTH(v.Fecha) = ?")
                params.append(mes)
            if anio:
                where_clauses.append("YEAR(v.Fecha) = ?")
                params.append(anio)
            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            offset = (page - 1) * per_page
            query = f"""
                SELECT v.VentaID, c.Nombre, v.Fecha, v.Total, v.MetodoPago, v.Observaciones
                FROM Ventas v
                JOIN Clientes c ON v.ClienteID = c.ClienteID
                {where_sql}
                ORDER BY v.Fecha DESC
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """
            params.extend([offset, per_page])
            cursor.execute(query, params)
            ventas = []
            for row in cursor.fetchall():
                ventas.append({
                    'id': row[0],
                    'cliente': row[1],
                    'fecha': row[2],
                    'total': row[3],
                    'metodo_pago': row[4],
                    'observaciones': row[5]
                })
            return ventas
        except Exception as e:
            print("Error al obtener historial de ventas:", e)
            return []
        finally:
            if self.connection:
                self.connection.close()

    def contar_historial_ventas(self, mes=None, anio=None):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            params = []
            where_clauses = []
            if mes:
                where_clauses.append("MONTH(Fecha) = ?")
                params.append(mes)
            if anio:
                where_clauses.append("YEAR(Fecha) = ?")
                params.append(anio)
            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            query = f"SELECT COUNT(*) FROM Ventas {where_sql}"
            cursor.execute(query, params)
            result = cursor.fetchone()
            return int(result[0]) if result else 0
        except Exception as e:
            print("Error al contar historial de ventas:", e)
            return 0
        finally:
            if self.connection:
                self.connection.close()

    def obtener_ventas_por_producto(self):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = """
                SELECT P.Nombre, SUM(DV.Cantidad) as TotalVendido
                FROM DetalleVenta DV
                JOIN Productos P ON DV.ProductoID = P.ProductoID
                JOIN Ventas V ON DV.VentaID = V.VentaID
                WHERE V.Fecha >= DATEADD(day, -7, GETDATE())
                GROUP BY P.Nombre
            """
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print("Error al obtener ventas por producto:", e)
            return []
        finally:
            if self.connection:
                self.connection.close()

    def obtener_productos_vendidos_hoy(self):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = """
                SELECT ISNULL(SUM(DV.Cantidad), 0)
                FROM DetalleVenta DV
                JOIN Ventas V ON DV.VentaID = V.VentaID
                WHERE CAST(V.Fecha AS DATE) = CAST(GETDATE() AS DATE)
            """
            cursor.execute(query)
            result = cursor.fetchone()
            return int(result[0]) if result and result[0] else 0
        except Exception as e:
            print("Error al obtener productos vendidos:", e)
            return 0
        finally:
            if self.connection:
                self.connection.close()

    

    def obtener_tipos_combustible_con_precio(self):
        try:
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            query = "SELECT TipoCombustibleID, Nombre, Precio FROM TiposCombustible"
            cursor.execute(query)
            tipos = []
            for row in cursor.fetchall():
                tipos.append({
                    'id': row[0],
                    'nombre': row[1],
                    'precio': row[2]
                })
            return tipos
        except Exception as e:
            print("Error al obtener tipos de combustible con precio:", e)
            return []
        finally:
            if self.connection:
                self.connection.close()

    def obtener_clientes_para_combustible(self):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        cursor.execute("SELECT ClienteID, Nombre FROM Clientes")
        clientes = []
        for row in cursor.fetchall():
            clientes.append({'id': row[0], 'nombre': row[1]})
        self.connection.close()
        return clientes
    
    def obtener_historial_ventas_combustible(self, cliente_id=None, fecha=None, pagina=1, por_pagina=10):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        filtros = []
        params = []
        if cliente_id:
            filtros.append("VC.ClienteID = ?")
            params.append(cliente_id)
        if fecha:
            filtros.append("CAST(VC.Fecha AS DATE) = ?")
            params.append(fecha)
        where = "WHERE " + " AND ".join(filtros) if filtros else ""
        offset = (pagina - 1) * por_pagina

        query = f"""
            SELECT VC.VentaCombustibleID, C.Nombre, VC.Fecha, TC.Nombre, DVC.CantidadLitros, DVC.PrecioUnitario, DVC.Subtotal, VC.Total, VC.MetodoPago
            FROM VentaCombustible VC
            JOIN Clientes C ON VC.ClienteID = C.ClienteID
            JOIN DetalleVentaCombustible DVC ON VC.VentaCombustibleID = DVC.VentaCombustibleID
            JOIN TiposCombustible TC ON DVC.TipoCombustibleID = TC.TipoCombustibleID
            {where}
            ORDER BY VC.Fecha DESC, VC.VentaCombustibleID DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """
        params.extend([offset, por_pagina])
        cursor.execute(query, params)
        historial = []
        for row in cursor.fetchall():
            # Formatear la fecha SIEMPRE como dd-mm-yyyy
            fecha_val = row[2]
            if isinstance(fecha_val, (datetime.datetime, datetime.date)):
                fecha_str = fecha_val.strftime('%d-%m-%Y')
            else:
                try:
                    fecha_str = datetime.datetime.strptime(str(fecha_val), '%Y-%m-%d').strftime('%d-%m-%Y')
                except Exception:
                    fecha_str = str(fecha_val)
            historial.append({
                'venta_id': row[0],
                'cliente': row[1],
                'fecha': fecha_str,
                'tipo_combustible': row[3],
                'litros': row[4],
                'precio': row[5],
                'subtotal': row[6],
                'total': row[7],
                'metodo_pago': row[8]
            })
        # Corregido aquí el nombre de la tabla
        count_query = f"SELECT COUNT(*) FROM VentaCombustible VC {where}"
        cursor.execute(count_query, params[:-2])
        total_registros = cursor.fetchone()[0]
        total_paginas = (total_registros + por_pagina - 1) // por_pagina
        self.connection.close()
        return historial, total_paginas

    def registrar_venta_combustible(self, cliente_id, fecha, metodo_pago, observaciones, detalles):
        try:
            print("Detalles recibidos para venta:", detalles)  # Depuración: muestra los detalles recibidos
            self.connection = pyodbc.connect(self._get_connection_string())
            cursor = self.connection.cursor()
            total = sum(float(d['subtotal']) for d in detalles)
            cursor.execute("""
                INSERT INTO VentaCombustible (ClienteID, Fecha, Total, MetodoPago, Observaciones)
                OUTPUT INSERTED.VentaCombustibleID
                VALUES (?, ?, ?, ?, ?)
            """, cliente_id, fecha, total, metodo_pago, observaciones)
            venta_id_row = cursor.fetchone()
            if not venta_id_row or not venta_id_row[0]:
                raise Exception("No se pudo obtener el ID de la venta insertada.")
            venta_id = int(venta_id_row[0])
            for d in detalles:
                print("Procesando detalle:", d)  # Depuración: muestra cada detalle procesado
                cursor.execute("""
                    INSERT INTO DetalleVentaCombustible
                    (VentaCombustibleID, TipoCombustibleID, PrecioUnitario, CantidadLitros, MontoQuetzales, Subtotal)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, venta_id, d['tipo_combustible_id'], d['precio_unitario'], d['cantidad_litros'], d['monto_quetzales'], d['subtotal'])

                inventario_inicial = self.obtener_ultimo_inventario_final(d['tipo_combustible_id'], cursor=cursor)
                entrada = 0.0
                salida = float(d['cantidad_litros'])
                inventario_final = inventario_inicial - salida
                self.agregar_registro_inventario(
                    d['tipo_combustible_id'],
                    inventario_inicial,
                    entrada,
                    salida,
                    inventario_final,
                    fecha,
                    es_automatico=1,
                    cursor=cursor
                )
            self.connection.commit()
            return True
        except Exception as e:
            print("Error al registrar venta:", e)
            if self.connection:
                self.connection.rollback()
            return False
        finally:
            if self.connection:
                self.connection.close()

    def obtener_ventas_combustible_filtrado(self, cliente_id=None, dia=None, mes=None, anio=None):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        query = """
            SELECT 
                MONTH(VC.Fecha) as mes,
                DAY(VC.Fecha) as dia,
                TC.Nombre as tipo,
                SUM(DVC.CantidadLitros) as total
            FROM VentaCombustible VC
            JOIN DetalleVentaCombustible DVC ON VC.VentaCombustibleID = DVC.VentaCombustibleID
            JOIN TiposCombustible TC ON DVC.TipoCombustibleID = TC.TipoCombustibleID
            WHERE 1=1
        """
        params = []
        if cliente_id:
            query += " AND VC.ClienteID = ?"
            params.append(cliente_id)
        if anio:
            query += " AND YEAR(VC.Fecha) = ?"
            params.append(anio)
        if mes:
            query += " AND MONTH(VC.Fecha) = ?"
            params.append(mes)
        if dia:
            query += " AND DAY(VC.Fecha) = ?"
            params.append(dia)
        query += " GROUP BY MONTH(VC.Fecha), DAY(VC.Fecha), TC.Nombre"
        cursor.execute(query, params)
        rows = cursor.fetchall()
        self.connection.close()
        return [
            {'mes': row[0], 'dia': row[1], 'tipo': row[2], 'total': row[3]}
            for row in rows
        ]
                
    def obtener_productos_mas_vendidos_filtrado(self, cliente_id=None, dia=None, mes=None, anio=None):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        query = """
            SELECT P.Nombre, SUM(DV.Cantidad) as cantidad
            FROM Ventas V
            JOIN DetalleVenta DV ON V.VentaID = DV.VentaID
            JOIN Productos P ON DV.ProductoID = P.ProductoID
            WHERE 1=1
        """
        params = []
        if cliente_id:
            query += " AND V.ClienteID = ?"
            params.append(cliente_id)
        if anio:
            query += " AND YEAR(V.Fecha) = ?"
            params.append(anio)
        if mes:
            query += " AND MONTH(V.Fecha) = ?"
            params.append(mes)
        if dia:
            query += " AND DAY(V.Fecha) = ?"
            params.append(dia)
        query += " GROUP BY P.Nombre ORDER BY cantidad DESC"
        cursor.execute(query, params)
        rows = cursor.fetchall()
        self.connection.close()
        return [{'nombre': row[0], 'cantidad': row[1]} for row in rows]

    def obtener_anios_ventas_combustible(self):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        cursor.execute("SELECT DISTINCT YEAR(Fecha) FROM VentaCombustible ORDER BY YEAR(Fecha)")
        anios = [str(row[0]) for row in cursor.fetchall()]
        self.connection.close()
        return anios

    def obtener_meses_disponibles(self, anio=None):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        if anio:
            cursor.execute("SELECT DISTINCT MONTH(Fecha) as mes FROM VentaCombustible WHERE YEAR(Fecha)=? ORDER BY mes", (anio,))
        else:
            cursor.execute("SELECT DISTINCT MONTH(Fecha) as mes FROM VentaCombustible ORDER BY mes")
        meses = [row[0] for row in cursor.fetchall()]
        self.connection.close()
        return meses
    
    def obtener_inventario_combustible(self, fecha_inicio, fecha_fin):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        query = """
            SELECT CONVERT(VARCHAR, Fecha, 23) AS Fecha, 
                (SELECT Nombre FROM TiposCombustible WHERE TipoCombustibleID = IC.TipoCombustibleID) AS Combustible,
                Entrada, Salida, InventarioFinal AS Saldo
            FROM InventarioCombustible IC
            WHERE Fecha BETWEEN ? AND ?
            ORDER BY Fecha, Combustible
        """
        cursor.execute(query, (fecha_inicio, fecha_fin))
        rows = cursor.fetchall()
        self.connection.close()
        return [
            {'Fecha': row[0], 'Combustible': row[1], 'Entrada': row[2], 'Salida': row[3], 'Saldo': row[4]}
            for row in rows
        ]
    
    def obtener_ventas_combustible(self, fecha_inicio, fecha_fin):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        query = """
            SELECT CONVERT(VARCHAR, VC.Fecha, 23) AS Fecha,
                C.Nombre AS Cliente,
                TC.Nombre AS Combustible,
                DVC.CantidadLitros AS Litros
            FROM VentaCombustible VC
            JOIN Clientes C ON VC.ClienteID = C.ClienteID
            JOIN DetalleVentaCombustible DVC ON VC.VentaCombustibleID = DVC.VentaCombustibleID
            JOIN TiposCombustible TC ON DVC.TipoCombustibleID = TC.TipoCombustibleID
            WHERE VC.Fecha BETWEEN ? AND ?
            ORDER BY VC.Fecha, C.Nombre
        """
        cursor.execute(query, (fecha_inicio, fecha_fin))
        rows = cursor.fetchall()
        self.connection.close()
        return [
            {'Fecha': row[0], 'Cliente': row[1], 'Combustible': row[2], 'Litros': row[3]}
            for row in rows
        ]
    
    def obtener_ventas_productos(self, fecha_inicio, fecha_fin):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        query = """
            SELECT CONVERT(VARCHAR, V.Fecha, 23) AS Fecha,
                P.Nombre AS Producto,
                DV.Cantidad,
                DV.Subtotal AS Total
            FROM Ventas V
            JOIN DetalleVenta DV ON V.VentaID = DV.VentaID
            JOIN Productos P ON DV.ProductoID = P.ProductoID
            WHERE V.Fecha BETWEEN ? AND ?
            ORDER BY V.Fecha, P.Nombre
        """
        cursor.execute(query, (fecha_inicio, fecha_fin))
        rows = cursor.fetchall()
        self.connection.close()
        return [
            {'Fecha': row[0], 'Producto': row[1], 'Cantidad': row[2], 'Total': row[3]}
            for row in rows
        ]
    
    def obtener_inventario_productos(self, fecha_inicio, fecha_fin):
        self.connection = pyodbc.connect(self._get_connection_string())
        cursor = self.connection.cursor()
        query = """
            SELECT 
                P.Nombre AS Producto,
                P.Cantidad AS Saldo
            FROM Productos P
            ORDER BY P.Nombre
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        self.connection.close()
        return [
            {'Producto': row[0], 'Saldo': row[1]}
            for row in rows
        ]

def main():
    authenticator = DatabaseAuthenticator()
    
    print("\n=== Sistema de Autenticación ===")
    print("=" * 30)
    
    username = input("Nombre de usuario: ")
    password = getpass.getpass("Contraseña: ")  # Oculta la contraseña al escribir
    
    if authenticator.authenticate_user(username, password):
        print("\n✅ Inicio de sesión exitoso. ¡Bienvenido!")
    else:
        print("\n❌ Error: Usuario o contraseña incorrectos")

if __name__ == "__main__":
    main()
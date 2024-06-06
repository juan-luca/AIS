using BusComun;
using Comun;
using System;
using System.Collections;
using System.Data;

namespace Inchost
{
    public class Inchost : cBase
    {
        /// <summary>
        /// Lista de proceso en el mismo orden que en el fichero de configuración
        /// </summary>
        private enum SubProcesos
        {
            ALTACONV = 1,
            PAGOS,
            RELACIONES,
            OBLIGACIONES,
            ATRIBOBLG,
            CAMBIO,
            DEUDA,
            CLIENTES,
            ATRIBCLIE,
            DOCSCLIE,
            DIRECCIONES,
            TELEFONOS,
            MAILS,
            TARJETAS,
            CTACTE,
            PRESTAMOS,
            CUOTAS,
            GARANTIAS,
            ACTUALIZACIONCONV,
            LOTES,
            ESTRATEGIAS,
            ASIGNACION,
            REFINANCIACION,
            HISTORICO,
            DEPURADOR,
        }

        private ArrayList Procesos;

        public int EjecuarInchost(string[] args)
        {
            int nRet = 0;

            base.SetConfiguracionRegional();

            cGlobales.ArchivoLog = "RECOBRO.LOG";

            /******************************************************************************** 
             * Cargo datos necesarios desde XML de inicializacion y Parametros de Entrada 
             ********************************************************************************/
            if ((nRet = CargoParametrosDeInicio(args)) != 0)
                return nRet;

            /******************************************************************************** 
             * Conexion a la base de datos 
             ********************************************************************************/
            conn = new cConexion(cGlobales.XMLConex);
            conn.Conectar();

            if (conn.Errores.Cantidad() != 0)
            {
                cIncidencia.Generar(conn.Errores, "Error de conexion a la BD");
                return -1;
            }

            /******************************************************************************** 
             * Preparacion de archivo de incidencia 
             ********************************************************************************/

            cGlobales.FicheroIncid = cGlobales.ComponerNombreArchivo(cGlobales.FicheroIncid, cGlobales.Hoy, cGlobales.sExtFichDatos);
            cIncidencia.CrearArchivoIncidencia(cGlobales.FicheroIncid);

            /******************************************************************************** 
             * Inicializacion de fechas de proceso
             ********************************************************************************/

            conn.ComienzoTransaccion();
            cFechaHabil FecHabil = new cFechaHabil(conn);
            if (FecHabil.InicializarFechaProceso(cGlobales.Hoy))
                conn.CommitTransaccion();
            else
            {
                cIncidencia.Aviso("Error inicializando fechas de proceso");
                conn.RollbackTransaccion();
                conn.Desconectar();
                return -2;
            }


            /******************************************************************************** 
             * Proceso de Pagos
             ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.PAGOS))
            {
                cIncidencia.Aviso("Empieza proceso de Movimientos");
                conn.ComienzoTransaccion();

                BusInchost.Pagos vPagos = new BusInchost.Pagos(conn);

                if (vPagos.ProcesoPagos())
                {
                    cIncidencia.Aviso("Finaliza proceso de Movimientos");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Movimientos");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.PAGOS;
                }
            }

            /******************************************************************************** 
             * Proceso Relaciones
             ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.RELACIONES))
            {
                cIncidencia.Aviso("Empieza proceso de Relaciones");
                conn.ComienzoTransaccion();

                BusInchost.Relaciones vRelac = new BusInchost.Relaciones(conn);

                if (vRelac.ProcesoRelaciones())
                {
                    cIncidencia.Aviso("Finaliza proceso de Relaciones");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Relaciones");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.RELACIONES;
                }
            }

            /******************************************************************************** 
            * Proceso Obligaciones
            *********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.OBLIGACIONES))
            {
                cIncidencia.Aviso("Empieza proceso de Obligaciones");
                conn.ComienzoTransaccion();

                BusInchost.Obligaciones vObligaciones = new BusInchost.Obligaciones(conn);

                if (vObligaciones.ProcesoObligaciones())
                {
                    cIncidencia.Aviso("Finaliza proceso de Obligaciones");
                    conn.CommitTransaccion();

                    cIncidencia.Aviso("Empieza tratamiento Obligaciones sin actualizar");
                    if (vObligaciones.ObligacionesNoActualizadas())
                    {
                        cIncidencia.Aviso("Finaliza tratamiento Obligaciones sin actualizar");
                        conn.CommitTransaccion();
                    }
                    else
                    {
                        cIncidencia.Aviso("Error tratamiento Obligaciones sin actualizar");
                        conn.RollbackTransaccion();
                        conn.Desconectar();
                        return (int)SubProcesos.OBLIGACIONES;
                    }
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Obligaciones");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.OBLIGACIONES;
                }
            }

            /******************************************************************************** 
            * Proceso Obligaciones
            *********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.ATRIBOBLG))
            {
                cIncidencia.Aviso("Empieza proceso de Atributos Obligaciones");
                conn.ComienzoTransaccion();

                BusInchost.Obligaciones vObligaciones = new BusInchost.Obligaciones(conn);

                if (vObligaciones.ProcesoAtributos())
                {
                    cIncidencia.Aviso("Finaliza proceso de Atributos Obligaciones");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Atributos Obligaciones");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.ATRIBOBLG;
                }
            }

            /******************************************************************************** 
            * Proceso Cambio
            ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.CAMBIO))
            {
                cIncidencia.Aviso("Empieza proceso de Cotizaciones");
                conn.ComienzoTransaccion();

                BusInchost.Cambio vCambio = new BusInchost.Cambio(conn);

                if (vCambio.ProcesaCambio())
                {
                    cIncidencia.Aviso("Finaliza proceso de Cotizaciones");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Cotizaciones");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.CAMBIO;
                }
            }

            /******************************************************************************** 
             * Proceso de Deudas
             ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.DEUDA))
            {
                cIncidencia.Aviso("Empieza proceso de Deuda");
                conn.ComienzoTransaccion();

                BusInchost.Deudas vDeuda = new BusInchost.Deudas(conn);

                if (vDeuda.ProcesoDeudas())
                {
                    cIncidencia.Aviso("Finaliza proceso de Deuda");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Deuda");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.DEUDA;
                }
            }

            /******************************************************************************** 
             * Proceso Clientes
             ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.CLIENTES))
            {
                cIncidencia.Aviso("Empieza proceso de Clientes");
                conn.ComienzoTransaccion();

                BusInchost.Clientes vClientes = new BusInchost.Clientes(conn);

                if (vClientes.ProcesoClientes())
                {
                    cIncidencia.Aviso("Finaliza proceso de Clientes");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Clientes");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.CLIENTES;
                }
            }

            /******************************************************************************** 
             * Proceso Atributos Clientes
             ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.ATRIBCLIE))
            {
                cIncidencia.Aviso("Empieza proceso de Atributos Clientes");
                conn.ComienzoTransaccion();

                BusInchost.Clientes vClientes = new BusInchost.Clientes(conn);

                if (vClientes.ProcesoAtributosClientes())
                {
                    cIncidencia.Aviso("Finaliza proceso de Atributos Clientes");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Atributos Clientes");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.CLIENTES;
                }
            }

            /******************************************************************************** 
             * Proceso Documentos Clientes
             ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.DOCSCLIE))
            {
                cIncidencia.Aviso("Empieza proceso de Documentos de Clientes");
                conn.ComienzoTransaccion();

                BusInchost.Clientes vClientes = new BusInchost.Clientes(conn);

                if (vClientes.ProcesoDocumentosClie())
                {
                    cIncidencia.Aviso("Empieza proceso de Documentos de Clientes");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Clientes");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.CLIENTES;
                }
            }

            /******************************************************************************** 
             * Proceso Direcciones
             ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.DIRECCIONES))
            {
                cIncidencia.Aviso("Empieza proceso de Direcciones");
                conn.ComienzoTransaccion();

                BusInchost.Direcciones vDirecciones = new BusInchost.Direcciones(conn);
                
                if (vDirecciones.ProcesoDirecciones())
                {
                    cIncidencia.Aviso("Finaliza proceso de Direcciones");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Direcciones");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.DIRECCIONES;
                }
            }

            /******************************************************************************** 
             * Proceso Telefonos
             ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.TELEFONOS))
            {
                cIncidencia.Aviso("Empieza proceso de Telefonos");
                conn.ComienzoTransaccion();

                BusInchost.Telefonos vTelefonos = new BusInchost.Telefonos(conn);

                if (vTelefonos.ProcesoTelefonos())
                {
                    cIncidencia.Aviso("Finaliza proceso de Telefonos");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Telefonos");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.TELEFONOS;
                }
            }

            /******************************************************************************** 
             * Proceso Mails
             ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.MAILS))
            {
                cIncidencia.Aviso("Empieza proceso de Mails");
                conn.ComienzoTransaccion();

                BusInchost.Mails vMails = new BusInchost.Mails(conn);

                if (vMails.ProcesoMails())
                {
                    cIncidencia.Aviso("Finaliza proceso de Mails");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Mails");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.MAILS;
                }
            }

            /******************************************************************************** 
            * Proceso Tarjetas
            ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.TARJETAS))
            {
                cIncidencia.Aviso("Empieza proceso de Tarjetas");
                conn.ComienzoTransaccion();

                BusInchost.Tarjetas vTarj = new BusInchost.Tarjetas(conn);

                if (vTarj.ProcesoTarjetas())
                {
                    cIncidencia.Aviso("Finaliza proceso de Tarjetas");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Tarjetas");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.TARJETAS;
                }
            }

            /******************************************************************************** 
            * Proceso CtaCte
            ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.CTACTE))
            {
                cIncidencia.Aviso("Empieza proceso de Cta.Cte.");
                conn.ComienzoTransaccion();

                BusInchost.CtaCte vCtaCte = new BusInchost.CtaCte(conn);

                if (vCtaCte.ProcesoCtaCte())
                {
                    cIncidencia.Aviso("Finaliza proceso de Cta.Cte.");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Cta.Cte.");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.CTACTE;
                }
            }

            /******************************************************************************** 
            * Proceso Prestamos
            ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.PRESTAMOS))
            {
                cIncidencia.Aviso("Empieza proceso de Prestamos");
                conn.ComienzoTransaccion();

                BusInchost.Prestamos vPrestamos = new BusInchost.Prestamos(conn);

                if (vPrestamos.ProcesoPrestamos())
                {
                    cIncidencia.Aviso("Finaliza proceso de Prestamos");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Prestamos");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.PRESTAMOS;
                }
            }

            /******************************************************************************** 
            * Proceso Cuotas
            ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.CUOTAS))
            {
                cIncidencia.Aviso("Empieza proceso de Cuotas");
                conn.ComienzoTransaccion();

                BusInchost.Cuotas vCuotas = new BusInchost.Cuotas(conn);

                if (vCuotas.ProcesoCuotas())
                {
                    cIncidencia.Aviso("Finaliza proceso de Cuotas");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Cuotas");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.CUOTAS;
                }
            }

            /******************************************************************************** 
            * Proceso Garantias
            ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.GARANTIAS))
            {
                cIncidencia.Aviso("Empieza proceso de Garantias");
                conn.ComienzoTransaccion();

                BusInchost.Garantias vGarantias = new BusInchost.Garantias(conn);

                if (vGarantias.ProcesoGarantias())
                {
                    cIncidencia.Aviso("Finaliza proceso de Garantias");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Garantias");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.GARANTIAS;
                }
            }


            /******************************************************************************** 
            * Actualiza Estructuras Convenios de Pago
            ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.ACTUALIZACIONCONV))
            {
                cIncidencia.Aviso("Empieza proceso de Actualizacion Convenios y Compromisos de Pago");
                conn.ComienzoTransaccion();

                BusInchost.Pagos vPagos = new BusInchost.Pagos(conn);

                if (vPagos.CruzarPagos())
                {
                    cIncidencia.Aviso("Finaliza proceso de Actualizacion Convenios y Compromisos de Pago");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Actualizacion Convenios y Compromisos de Pago");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.PAGOS;
                }
            }

            /******************************************************************************** 
            * Proceso Lider y Lotes
            ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.LOTES))
            {
                cIncidencia.Aviso("Empieza proceso de Lotes");
                conn.ComienzoTransaccion();

                BusInchost.Lotes vLotes = new BusInchost.Lotes(conn);

                if (vLotes.ProcesoLotes())
                {
                    cIncidencia.Aviso("Finaliza proceso de Lotes");
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso de Lotes");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.LOTES;
                }
            }

            /******************************************************************************** 
            * Proceso Selector de Estrategias
            ********************************************************************************/
            if (HayQueEjecSubProceso((int)SubProcesos.ESTRATEGIAS))
            {
                cIncidencia.Aviso("Empieza proceso Selector de Estrategias");
                conn.ComienzoTransaccion();

                BusMotor.Motor vMotor = new BusMotor.Motor(conn);

                if (vMotor.ProcesoSelectorEstrategias())
                {
                    conn.CommitTransaccion();
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso Selector de Estrategias");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.ESTRATEGIAS;
                }
            }

            if (HayQueEjecSubProceso((int)SubProcesos.ASIGNACION))
            {
                cIncidencia.Aviso("Comienza Asignacion de Estrategias");
                conn.ComienzoTransaccion();

                BusInchost.Lotes vLotes = new BusInchost.Lotes(conn);

                if (vLotes.AsignarEstrategias())
                {
                    conn.CommitTransaccion();
                    cIncidencia.Aviso("Finaliza proceso Selector de Estrategias");
                }

                else
                {
                    cIncidencia.Aviso("Error en proceso Selector de Estrategias");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.ESTRATEGIAS;
                }
            }

            if (HayQueEjecSubProceso((int)SubProcesos.REFINANCIACION))
            {
                cIncidencia.Aviso("Comienza el proceso de Recálculo de Cuotas de Refinanciaciones");
                conn.ComienzoTransaccion();

                BusInchost.Refinanciacion vRefinanciacion = new BusInchost.Refinanciacion(conn);

                if (vRefinanciacion.ProcesoRecalculoCuotasRefinanciacion())
                {
                    conn.CommitTransaccion();
                    cIncidencia.Aviso("Finaliza proceso Recálculo de Cuotas de Refinanciaciones");
                }
                else
                {
                    cIncidencia.Aviso("Error en proceso Recálculo de Cuotas de Refinanciaciones");
                    conn.RollbackTransaccion();
                    conn.Desconectar();
                    return (int)SubProcesos.REFINANCIACION;
                }
            }

            /******************************************************************************** 
            * Proceso Historico
            ********************************************************************************/
            //if (HayQueEjecSubProceso((int)SubProcesos.HISTORICO))
            //{
            //    cIncidencia.Aviso("Empieza proceso Historico");
            //    conn.ComienzoTransaccion();

            //    BusInchost.Historicos vHistoricos = new BusInchost.Historicos(conn);

            //    if (vHistoricos.ProcesoHistoricos())
            //    {
            //        cIncidencia.Aviso("Finaliza proceso Historico");
            //        conn.CommitTransaccion();
            //    }
            //    else
            //    {
            //        cIncidencia.Aviso("Error en proceso Historico");
            //        conn.RollbackTransaccion();
            //        conn.Desconectar();
            //        return (int)SubProcesos.HISTORICO;
            //    }
            //}

            /******************************************************************************** 
            * Proceso Depurador
            ********************************************************************************/
            //if (HayQueEjecSubProceso((int)SubProcesos.DEPURADOR))
            //{
            //    cIncidencia.Aviso("Empieza proceso Depuracion");
            //    conn.ComienzoTransaccion();

            //    BusInchost.Historicos vHistoricos = new BusInchost.Historicos(conn);

            //    if (vHistoricos.ProcesoDepuracion())
            //    {
            //        cIncidencia.Aviso("Finaliza proceso Depuracion");
            //        conn.CommitTransaccion();
            //    }
            //    else
            //    {
            //        cIncidencia.Aviso("Error en proceso Depuracion");
            //        conn.RollbackTransaccion();
            //        conn.Desconectar();
            //        return (int)SubProcesos.DEPURADOR;
            //    }
            //}

            /******************************************************************************** 
            * Actualizo Fecha de Proceso
            ********************************************************************************/
            conn.ComienzoTransaccion();
            FecHabil = new cFechaHabil(conn);
            if (FecHabil.ActualizarIndiceFechasHabilesBD(1))
            {
                FecHabil.ActualizarUltFechaProceso(cGlobales.Hoy);
                conn.CommitTransaccion();
            }
            else
            {
                cIncidencia.Aviso("Error actualizando fechas de proceso");
                conn.RollbackTransaccion();
                conn.Desconectar();
                return -2;
            }

            /******************************************************************************** 
            * Desconecto de  la base de datos y finalizo el proceso con error level en 0
            ********************************************************************************/
            conn.Desconectar();

            return 0;

        }

        /******************************************************************************** 
         RECUPERO LA INFORMACION DE ARRANQUE DEL PROCESO.
         SETEO VARIABLES QUE VOY A UTILIZAR A LO LARGO DE TODOS EL PROCESO.
        ********************************************************************************/
        private int CargoParametrosDeInicio(string[] args)
        {
            try
            {
                cGlobales.DirEjecucion = AppDomain.CurrentDomain.BaseDirectory + "..\\";

                if (args.Length >= 2)
                    cGlobales.XMLProceso = args[0].ToString();
                else
                {
                    cIncidencia.AvisoAConsola("Uso del programa:");
                    cIncidencia.AvisoAConsola("  RSActBD <fichero_XML_inicializacion> [<fecha> [<ultimo proceso>]]");
                    cIncidencia.AvisoAConsola("  Parametros:");
                    cIncidencia.AvisoAConsola("   <fichero_XML_inicializacion> : Ruta del fichero XML de inicializacion del procesos RSActBD");
                    cIncidencia.AvisoAConsola("   <fecha> : Fecha de los archivos de datos del Host en formato AAAAMMDD  [Fecha Actual]");
                    cIncidencia.AvisoAConsola("   <ultimo proceso> : ultimo proceso ejecutado correctamente (para reenganches) [0]");
                    return -1;
                }

                if (args.Length >= 2)
                    cGlobales.Hoy = args[1].ToString();
                else
                {
                    cGlobales.Hoy = cFormat.FechaPcToBD(DateTime.Today.ToShortDateString());
                    cIncidencia.Aviso("Se toma la fecha de hoy.");
                }

                /* Recupera el ultimo proceso ejecutado */
                if (args.Length >= 3)
                    cGlobales.nUltimoProceso = Convert.ToInt32(args[2].ToString());
                else
                    cGlobales.nUltimoProceso = 0;

                if (cGlobales.nUltimoProceso < 0)
                    cGlobales.nUltimoProceso = 0;

                cGlobales.HoraMotor = DateTime.Now.ToString("HHmmss");
            }
            catch
            {
                // Error al recuperar parametros 
                cIncidencia.Aviso("MAIN: error al recuperar parametros del programa");
                return -1;
            }

            try
            {
                /* Leo XML de configuracion*/
                DataSet ds = new DataSet();
                DataTable Tabla;
                ds.ReadXml(cGlobales.XMLProceso);

                try
                {
                    Tabla = ds.Tables["ConfiguracionLog"];
                    cGlobales.ArchivoLog = Tabla.Rows[0]["ArchivoLog"].ToString();
                    cGlobales.sDirLog = Tabla.Rows[0]["DirLog"].ToString();

                    if (!cGlobales.sDirLog.EndsWith("\\"))
                        cGlobales.sDirLog += "\\";
                }
                catch
                {
                    cGlobales.ArchivoLog = "RECOBRO.LOG";
                    cGlobales.sDirLog = cGlobales.DirEjecucion;
                }

                /*  Recupero parametros de conexion*/
                Tabla = ds.Tables["Conexion"];
                cGlobales.XMLConex = Tabla.Rows[0]["Fichero"].ToString();

                // RECUPERO VARIABLES CONFIGURACION
                Tabla = ds.Tables["Parametros"];
                cGlobales.FicheroIncid = Tabla.Rows[0]["FicheroIncid"].ToString();  //"INCIDENCIAS.TXT";
                cGlobales.sExtFichDatos = Tabla.Rows[0]["ExtFichDatos"].ToString();
                cGlobales.PctMinEficCP = Convert.ToDouble(Tabla.Rows[0]["PctMinEficCP"].ToString());
                try
                {
                    cGlobales.nMarcaVelocidad = Convert.ToInt64(Tabla.Rows[0]["MarcaVelocidad"].ToString());
                    cGlobales.nFrecuenciaCommit = Convert.ToInt64(Tabla.Rows[0]["FrecuenciaCommit"].ToString());
                }
                catch
                {
                    cGlobales.nMarcaVelocidad = 99000000;
                    cGlobales.nFrecuenciaCommit = 10000;
                }

                Tabla = ds.Tables["AsigEstrategia"];
                try
                {
                    cGlobales.nAsigEstratDefault = Convert.ToInt32(Tabla.Rows[0]["Default"].ToString());
                }
                catch
                {
                    cGlobales.nAsigEstratDefault = 1;
                }

                if (cGlobales.nAsigEstratDefault == 1)
                {
                    cGlobales.GrupoNoMora = Tabla.Rows[0]["GrupoNoMora"].ToString();
                    cGlobales.GrupoMora = Tabla.Rows[0]["GrupoMora"].ToString();
                }
                cGlobales.EstrategiaNoProceso = "";
                cGlobales.DiasMora = Convert.ToInt32(Tabla.Rows[0]["DiasMora"].ToString());

                try
                {
                    cGlobales.bTrazarLotes = Tabla.Rows[0]["trazarLotes"].ToString() == "1";
                    cGlobales.bFiltroLotes = Convert.ToInt32(Tabla.Rows[0]["FiltroLotes"].ToString());
                }
                catch
                {
                    cGlobales.bTrazarLotes = false;
                    cGlobales.bFiltroLotes = 0;
                }

                Tabla = ds.Tables["MultiThread"];
                cGlobales.nHilosClientes = Convert.ToInt32(Tabla.Rows[0]["HilosClientes"].ToString());
                cGlobales.nClientesXHilo = Convert.ToInt32(Tabla.Rows[0]["ClientesXHilo"].ToString());

                cGlobales.nHilosTelefonos = Convert.ToInt32(Tabla.Rows[0]["HilosTelefonos"].ToString());
                cGlobales.nTelefonosXHilo = Convert.ToInt32(Tabla.Rows[0]["TelefonosXHilo"].ToString());

                cGlobales.nHilosDirecciones = Convert.ToInt32(Tabla.Rows[0]["HilosDirecciones"].ToString());
                cGlobales.nDireccionesXHilo = Convert.ToInt32(Tabla.Rows[0]["DireccionesXHilo"].ToString());

                cGlobales.nHilosMails = Convert.ToInt32(Tabla.Rows[0]["HilosMails"].ToString());
                cGlobales.nMailsXHilo = Convert.ToInt32(Tabla.Rows[0]["MailsXHilo"].ToString());

                cGlobales.nHilosGarantias = Convert.ToInt32(Tabla.Rows[0]["HilosGarantias"].ToString());
                cGlobales.nGarantiasXHilo = Convert.ToInt32(Tabla.Rows[0]["GarantiasXHilo"].ToString());

                cGlobales.nHilosObligaciones = Convert.ToInt32(Tabla.Rows[0]["HilosObligaciones"].ToString());
                cGlobales.nObligacionesXHilo = Convert.ToInt32(Tabla.Rows[0]["ObligacionesXHilo"].ToString());

                cGlobales.nHilosLotes = Convert.ToInt32(Tabla.Rows[0]["HilosLotes"].ToString());
                cGlobales.nLotesXHilo = Convert.ToInt32(Tabla.Rows[0]["LotesXHilo"].ToString());

                cGlobales.nHilosSelecEstr = Convert.ToInt32(Tabla.Rows[0]["HilosSelecEstr"].ToString());
                cGlobales.nSelecEstrLotesXHilo = Convert.ToInt32(Tabla.Rows[0]["SelecEstrLotesXHilo"].ToString());

            }
            catch(Exception ex)
            {
                // Error con el XML de inicializacion del INCHOST
                cIncidencia.Aviso("MAIN: error al abrir el archivo XML de inicializacion");
                return -1;
            }

            CargoTipoConexcion();

            return 0;

        }

        private void CargoTipoConexcion()
        {
            DataSet ds = new DataSet();
            DataTable Tabla;
            ds.ReadXml(cGlobales.XMLProceso);

            Procesos = new ArrayList();

            Tabla = ds.Tables["Procesos"];

            for (int i = 0; i < Tabla.Columns.Count; i++)
            {
                if (i >= cGlobales.nUltimoProceso)
                    Procesos.Add(Convert.ToInt32(Tabla.Rows[0][i].ToString()));
                else
                    Procesos.Add(Convert.ToInt32("0"));
            }

        }

        private bool HayQueEjecSubProceso(int nProceso)
        {
            try
            {
                return (Convert.ToInt32(Procesos[nProceso - 1].ToString()) == 1);
            }
            catch
            {
                return false;
            }
        }
    }
}

using BusComun;
using Comun;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;

namespace BusInchost
{
    public class Obligaciones : cBase
    {

        public Obligaciones(cConexion pconn)
        {
            conn = pconn;
        }

        public bool ProcesoObligaciones()
        {
            return CargaMasiva();
        }

        #region Carga Unitaria

        /// <summary>
        /// Carga unitaria de Obligaciones
        /// </summary>
        private bool CargaUnitaria()
        {
            try
            {
                long nContTrat = 0;
                long nContOK = 0;
                string sClienteAnt = "-1";

                ObligacionesDalc ObligacionesD = new ObligacionesDalc(conn);

                TyObligacion Obligacion = new TyObligacion();

                List<TyObligacion> ObligacionesCliente = new List<TyObligacion>();

                AISDataReader DrC = new AISDataReader();

                // Uso una segunda conexion a la base para el DataReader Principal
                cConexion connObligaciones = new cConexion(cGlobales.XMLConex);
                connObligaciones.Conectar();
                if (connObligaciones.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(connObligaciones.Errores, "OPENBDOBLG", "No se pudo abrir la conexion");
                }

                //Abro el cursor de Obligaciones recibidas en la tabla de intercambio
                ObligacionesDalc vObligacionBD = new ObligacionesDalc(connObligaciones);
                DrC = vObligacionBD.AbrirCursorObligaciones();

                if (vObligacionBD.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(vObligacionBD.Errores, "OPENOBLG", "Error al abrir cursor de Obligaciones");
                    return false;
                }

                cIncidencia.Aviso("Fin Apertura Cursor Obligaciones");

                // Establezco la cantidad de hilos que se van a utilizar y los creo 
                Hilos HilosObligacion = new Hilos(cGlobales.nObligacionesXHilo, cGlobales.nHilosObligaciones);

                // Conecto los hilos a la base de datos
                if (!HilosObligacion.ConectarHilosABaseDeDatos(cGlobales.XMLConex))
                    return false;

                while (vObligacionBD.FechObligacion(DrC, ref Obligacion))
                {

                    nContTrat++;
                    if (sClienteAnt != Obligacion.CLCOD)
                    {
                        sClienteAnt = Obligacion.CLCOD;
                        if (ObligacionesCliente.Count > 0)
                        {
                            // Cargo el elemento en alguno de los hilos disponible
                            // Si la CargoElementoEnAlgunHilo retorna falso entonces indica que los hilos completos 
                            // y listos para ejecutar
                            if (!HilosObligacion.CargoElementoEnAlgunHilo(ObligacionesCliente))
                                HilosObligacion.Arrancar(new ParameterizedThreadStart(FuncionArranqueHilo));
                        }
                        ObligacionesCliente = new List<TyObligacion>();
                    }
                    ObligacionesCliente.Add(Obligacion);

                    cIncidencia.SetMarcaVelocidad(nContTrat);
                }

                /* Cargo los Direcciones del ultimo cliente*/
                if (ObligacionesCliente.Count > 0)
                    HilosObligacion.CargoElementoEnAlgunHilo(ObligacionesCliente);

                HilosObligacion.Arrancar(new ParameterizedThreadStart(FuncionArranqueHilo));

                DrC.Close();
                connObligaciones.Desconectar();

                if (vObligacionBD.Errores.Cantidad() != 0)
                    return false;
                else
                {
                    if (HilosObligacion.EjecucionDeLosHilosOk())
                    {
                        nContTrat = HilosObligacion.ElemenosTratados();
                        nContOK = HilosObligacion.ElemenosTratadosOk();

                        HilosObligacion.CommitDeLosHilos();
                        HilosObligacion.DesConectarHilosABaseDeDatos();
                        cIncidencia.Aviso("Obligaciones: Tratados -> " + nContTrat.ToString() + " OK -> " + nContOK);
                        return true;
                    }
                    else
                    {
                        HilosObligacion.RollbackDeLosHilos();
                        HilosObligacion.DesConectarHilosABaseDeDatos();
                        return false;
                    }
                }
            }
            catch (Exception e)
            {
                cIncidencia.Aviso("Se produjo un error inesperado en la administracion de hilos. " + e.Message);
                return false;
            }
        }

        public void FuncionArranqueHilo(object obj)
        {
            Obligaciones ObligacionActual;
            HiloAIS Hilo;

            Hilo = (HiloAIS)obj;
            ObligacionActual = new Obligaciones(Hilo.Conexion);

            ObligacionActual.TratarObligacionLista(obj);

            return;
        }

        public void TratarObligacionLista(object obj)
        {
            HiloAIS Hilo;
            string sIncidencia = "";

            Hilo = (HiloAIS)obj;

            List<object> ListaClientes = Hilo.ListaElementosAIS;

            foreach (List<TyObligacion> ListaObligaciones in ListaClientes)
            {
                foreach (TyObligacion Obligacion in ListaObligaciones)
                {
                    Hilo.nContTratados++;
                    if (TrataObligacion(Obligacion))
                        Hilo.nContOk++;
                    else
                    {
                        sIncidencia = "OBLG" + "|" + Obligacion.OGCOD;
                        cIncidencia.IncidenciaInterfaces(cGlobales.FicheroIncid, sIncidencia);
                    }
                    if ((Hilo.nContTratados % cGlobales.nFrecuenciaCommit) == 0)
                    {
                        conn.CommitTransaccion();
                        conn.ComienzoTransaccion();
                    }
                }
            }
            Hilo.bEjecutadoOk = (Hilo.bEjecutadoOk && true);
            conn.CommitTransaccion();

            return;
        }

        public bool TrataObligacion(TyObligacion Obligacion)
        {
            if (!TratoModificacionObligacion(Obligacion))
                return false;

            return true;
        }

        public bool TratoBajaObligacionRHISTO(TyObligacion Obligacion, string sCliente)
        {

            //ObligacionesDalc ObligD = new ObligacionesDalc(conn);

            //tyObligacion BDObligacion = new tyObligacion();

            //BDObligacion.OGCOD = Obligacion.OGCOD;

            //// Si no encuentro la obligacion en la base de datos se procesará como alta
            //if (!ObligD.CargaParcialObligModBD(BDObligacion))
            //{
            //    cIncidencia.Aviso("No se encuentra la obligacion que se quiere dar de baja. Obligacion: " + Obligacion.OGCOD);
            //    return false;
            //}

            //if (sCliente == "")
            //{
            //    // Obtengo el cliente titular principal
            //    Obligacion.CLCOD = ObligD.GetCodigoCliente(Obligacion.OGCOD);
            //}
            //else
            //    Obligacion.CLCOD = sCliente;


            //if (BDObligacion.OGFECMOR != "")
            //{
            //    /* Cargamos el registro historico (si existe) */
            //    HistoricosDalc HistD = new HistoricosDalc(conn);
            //    Historicos Hist = new Historicos(conn);
            //    tyHistorico Historico = new tyHistorico();
            //    /* La obligacion se ha regularizado */
            //    /* Cargamos el registro historico (si existe) */
            //    Historico.HIOBLIG = Obligacion.OGCOD;
            //    if (HistD.CargarHistorico(Historico))
            //    {
            //        /* Preparacion de datos del nuevo registro historico (no existia) */
            //        Hist.GeneraHistoricoSalida(BDObligacion, Historico);
            //    }

            //    if (Obligacion.OGMOTIVOBAJA == Const.OBLG_MARCA_BAJA_JUDICIAL)
            //    {
            //        Obligacion.OGESTADO = Const.OBLG_MARCA_BAJA_JUDICIAL;
            //        InsertoGestionNovedadObligacion(Obligacion, Const.TAR_CANCEL_PASE_MORA, "235000");
            //    }
            //    else if (Obligacion.OGMOTIVOBAJA == Const.OBLG_MARCA_BAJA_REFORMULACION)
            //    {
            //        Obligacion.OGESTADO = Const.OBLG_MARCA_BAJA_REFORMULACION;
            //        InsertoGestionNovedadObligacion(Obligacion, Const.TAR_REFORMULACION, "235000");
            //    }
            //    else if (Obligacion.OGMOTIVOBAJA == Const.OBLG_MARCA_BAJA_REFINANCIACION)
            //    {
            //        Obligacion.OGESTADO = Const.OBLG_MARCA_BAJA_REFINANCIACION;
            //        InsertoGestionNovedadObligacion(Obligacion, Const.TAR_REFINANCIACION, "235000");
            //    }
            //    else if (Obligacion.OGMOTIVOBAJA == Const.OBLG_MARCA_BAJA_OTROS)
            //    {
            //        Obligacion.OGESTADO = Const.OBLG_MARCA_BAJA_OTROS;
            //        InsertoGestionNovedadObligacion(Obligacion, Const.TAR_BAJAOTROS, "235000");
            //    }
            //    else if (Obligacion.OGMOTIVOBAJA == Const.OBLG_MARCA_BAJA_NORMALIZACION)
            //    {
            //        Obligacion.OGESTADO = Const.OBLG_MARCA_BAJA_NORMALIZACION;
            //        InsertoGestionNovedadObligacion(Obligacion, Const.TAR_NORMALIZACION, "235000");
            //    }
            //    else
            //    {
            //        Obligacion.OGESTADO = Const.OBLG_MARCA_BAJA_OTROS;
            //        InsertoGestionNovedadObligacion(Obligacion, Const.TAR_BAJAOTROS, "235000");
            //    }
            //}
            //else
            //{
            //    Obligacion.OGESTADO = Const.OBLG_MARCA_REGISTRO_BAJA;
            //}

            //if (!ObligD.MarcarObligacionBajaBD(Obligacion))
            //    return false;

            return true;

        }

        public bool ProcesoAtributos()
        {
            bool retorno = false;
            ObligacionesDalc ObligacionesD = new ObligacionesDalc(conn);

            if (ObligacionesD.VaciarAtributos())
                retorno = ObligacionesD.CargarAtributos();

            return retorno;
        }

        public bool ObligacionesNoActualizadas()
        {
            bool retonor = false;
            ObligacionesDalc ObligacionesD = new ObligacionesDalc(conn);

            //Creamos las Gestiones FADT para las oblgiaciones que no se han actualizado en los últimos 5 días
            retonor = ObligacionesD.InsertarGestionesFADT();
            if (retonor)
                retonor = ObligacionesD.BorrarGestionesFADT();

            if (retonor)
                retonor = ObligacionesD.InicializarDeuda();

            if (retonor)
                retonor = ObligacionesD.InicializarFechaMora();

            if (retonor)
                retonor = retonor = ObligacionesD.InsertarGestionesRANA();

            return retonor;
        }

        public bool TratoModificacionObligacion(TyObligacion Obligacion)
        {
            bool retorno = false;
            Historicos Hist = new Historicos(conn);
            HistoricosDalc HistD = new HistoricosDalc(conn);
            ObligacionesDalc ObligD = new ObligacionesDalc(conn);

            TyObligacion BDObligacion = new TyObligacion
            {
                OGCOD = Obligacion.OGCOD
            };

            // Si no encuentro la obligacion en la base de datos se procesará como alta
            if (!ObligD.CargaParcialObligModBD(BDObligacion))
            {
                if (!TratoAltaObligacion(Obligacion))
                    retorno = false;
                else
                    retorno = true;
            }
            else
            {
                // Obtengo el cliente titular principal
                Obligacion.CLCOD = ObligD.GetCodigoCliente(Obligacion.OGCOD);

                if (BDObligacion.OGFECMOR == "" && Obligacion.OGFECMOR != "")
                {
                    if (Obligacion.RHisto.HIOBLIG != "")
                    {
                        /* Marcamos el registro como modificacion y reentrada en mora */
                        //Obligacion.OGESTADO = Const.OBLG_MARCA_MODIF_REENTRADA;

                        /* Insertamos un nuevo registro historico */
                        InsertoGestionNovedadObligacion(Obligacion, Const.TAR_REENTRADA, "235000");
                    }
                    else
                    {
                        /* Marcamos el registro como modificacion y entrada en mora */
                        //Obligacion.OGESTADO = Const.OBLG_MARCA_MODIF_ALTA;

                        HistD.InsertaHistorico(Obligacion.OGCOD, Obligacion.CLCOD);
                        /* Insertamos un nuevo registro historico */
                        InsertoGestionNovedadObligacion(Obligacion, Const.TAR_ENTRADA_MORA, "235000");
                    }
                }
                else if (BDObligacion.OGFECMOR != "" && Obligacion.OGFECMOR == "")
                {
                    // Si era morosa y ahora pasa a no morosa
                    TyHistorico Historico = new TyHistorico();

                    //Obligacion.OGESTADO = Const.OBLG_MARCA_MODIF_NORMALIZACION;

                    // La obligacion se ha regularizado 
                    // Cargamos el registro historico (si existe) 

                    // Preparacion de datos del nuevo registro historico (no existia) 
                    Hist.GeneraHistoricoSalida(BDObligacion, Obligacion.RHisto);
                    InsertoGestionNovedadObligacion(Obligacion, Const.TAR_NORMALIZACION, "235000");
                }
                else
                {
                    /* Marcamos el registro como modificación genérica */
                    //Obligacion.OGESTADO = Const.OBLG_MARCA_MODIF;
                }

                // Obtengo la region 
                //Obligacion.OGREGION = ObtengoRegionOblig(Obligacion.OGCOD);

                /* Actualizacion del registro */
                if (!ObligD.ActualizaObligacion(Obligacion))
                    retorno = false;
                else
                    retorno = true;
            }

            return retorno;
        }

        public bool TratoModificacionObligacionRHISTO(TyObligacion Obligacion, bool nProcesoDefault)
        {
            HistoricosDalc HistD = new HistoricosDalc(conn);
            Historicos Hist = new Historicos(conn);

            ObligacionesDalc ObligD = new ObligacionesDalc(conn);

            TyObligacion BDObligacion = new TyObligacion
            {
                OGCOD = Obligacion.OGCOD
            };

            // Si no encuentro la obligacion en la base de datos se procesará como alta
            if (!ObligD.CargaParcialObligModBD(BDObligacion))
            {
                if (!TratoAltaObligacion(Obligacion))
                    return false;
            }

            // Obtengo el cliente titular principal
            Obligacion.CLCOD = ObligD.GetCodigoCliente(Obligacion.OGCOD);


            if (BDObligacion.OGFECMOR == "" && Obligacion.OGFECMOR != "")
            {
                /* Si no tenia fecha de entrada en mora y ahora la tiene, 
                   la entrada en el sistema es hoy */

                Obligacion.OGFECENT = cGlobales.Hoy;

                if (HistD.ExisteEnHistorico(Obligacion.OGCOD))
                {
                    /* Insertamos un nuevo registro historico */
                    InsertoGestionNovedadObligacion(Obligacion, Const.TAR_REENTRADA, cGlobales.HoraMotor);
                }
                else
                {
                    HistD.InsertaHistorico(Obligacion.OGCOD, Obligacion.CLCOD);
                    /* Insertamos un nuevo registro historico */
                    InsertoGestionNovedadObligacion(Obligacion, Const.TAR_ENTRADA_MORA, cGlobales.HoraMotor);
                }

            }
            else if (BDObligacion.OGFECMOR != "" && Obligacion.OGFECMOR == "")
            {
                // Si era morosa y ahora pasa a no morosa

                TyHistorico Historico = new TyHistorico();
                Obligacion.OGFECENT = BDObligacion.OGFECENT;

                // La obligacion se ha regularizado 
                // Cargamos el registro historico (si existe) 
                Historico.HIOBLIG = Obligacion.OGCOD;
                if (HistD.CargarHistorico(Historico))
                {
                    // Preparacion de datos del nuevo registro historico (no existia) 
                    Hist.GeneraHistoricoSalida(BDObligacion, Historico);
                }
                InsertoGestionNovedadObligacion(Obligacion, Const.TAR_NORMALIZACION, cGlobales.HoraMotor);

            }
            else
            {
                Obligacion.OGFECENT = BDObligacion.OGFECENT;
                /* La fecha de entrada no varia */
                Obligacion.OGFECMOR = BDObligacion.OGFECMOR;
            }

            // Obtengo la region 
            //Obligacion.OGREGION = ObtengoRegionOblig(Obligacion.OGCOD);

            if (nProcesoDefault)
            {
                /* Actualizacion del registro */
                if (!ObligD.ActualizaObligacion(Obligacion))
                    return false;
            }
            else
            {
                /* SOLO PARA CONVENIO DE PAGO - Se Actualizan algunos campos */
                if (!ObligD.ActualizaObligacionTipoConvP(Obligacion))
                    return false;
            }

            return true;
        }

        public bool TratoAltaObligacion(TyObligacion Obligacion)
        {
            HistoricosDalc HistD = new HistoricosDalc(conn);
            ObligacionesDalc ObligD = new ObligacionesDalc(conn);

            // Si la obligacion esta en mora
            if (Obligacion.OGFECMOR != "")
            {
                // Obtengo el cliente titular principal
                Obligacion.CLCOD = ObligD.GetCodigoCliente(Obligacion.OGCOD);

                if (Obligacion.RHisto.HIOBLIG != "")
                {
                    // Insertamos un nuevo registro historico //
                    InsertoGestionNovedadObligacion(Obligacion, Const.TAR_REENTRADA, cGlobales.HoraMotor);
                }
                else
                {
                    HistD.InsertaHistorico(Obligacion.OGCOD, Obligacion.CLCOD);
                    // Insertamos un nuevo registro historico //
                    InsertoGestionNovedadObligacion(Obligacion, Const.TAR_ENTRADA_MORA, cGlobales.HoraMotor);
                }
            }

            // Insertamos la obligación //
            if (!ObligD.InsertaObligacion(Obligacion))
            {
                if (!ObligD.ActualizaObligacion(Obligacion))
                    return false;
            }
            else
            {
                //Registramos la entrada a RS de la obligación
                InsertoGestionNovedadObligacion(Obligacion, Const.TAR_ALTA_SISTEMA, cGlobales.HoraMotor);
            }

            return true;
        }

        private bool InsertoGestionNovedadObligacion(TyObligacion Obligacion, string sTar, string sHora)
        {
            ObligacionesDalc ObligD = new ObligacionesDalc(conn);
            LotesDalc loteD = new LotesDalc(this.conn);
            bool bSalir = false;
            int iHora = 0;
            bool nRet = false;

            tyGestion Gestion = new tyGestion();

            if (Obligacion.CLCOD != "")
            {
                Gestion.sBGLOTE = Obligacion.CLCOD;
                Gestion.sBGFECHA = cGlobales.Hoy;
                Gestion.sBGHORA = sHora;
                Gestion.sBGTARREA = sTar;
                Gestion.sBGGESCLIE = "0";
                Gestion.sBGOBLIG = Obligacion.OGCOD;
                Gestion.sBGFIGURA = "NP";

                DataSet dsLote = loteD.GetLote(Obligacion.CLCOD);
                if (dsLote.Tables["LOTE"].Rows.Count > 0)
                {
                    DataRow drLote = dsLote.Tables["LOTE"].Rows[0];

                    Gestion.sBGNMORANT = drLote["LONMORA"].ToString();
                    Gestion.sBGNMORNEW = drLote["LONMORA"].ToString();
                    Gestion.sBGOWNANT = drLote["LOPERFIL"].ToString();
                    Gestion.sBGOWNNEW = drLote["LOPERFIL"].ToString();
                    Gestion.sBGESTESCENANT = drLote["LOTURNO"].ToString();
                    Gestion.sBGESTESCENNEW = drLote["LOTURNO"].ToString();
                }

                EjecutarDalc Ejec = new EjecutarDalc(conn);
                while (!bSalir)
                {
                    nRet = Ejec.InsertarGestion(Gestion);
                    if (Ejec.Errores.Cantidad() != 0)
                    {
                        if (Ejec.Errores.Item(0).nCodigo == Const.SQL_CLAVE_DUPLICADA)
                        {
                            iHora++;
                            Gestion.sBGHORA = Gestion.sBGHORA.Substring(0, 4) + iHora.ToString().PadLeft(2, '0');
                        }
                        else
                            bSalir = true;
                    }
                    else
                        bSalir = true;
                }

                return nRet;
            }

            return false;
        }

        internal DataSet CargarObligacionesDelLote(string sLote)
        {
            DataSet Ds = new DataSet();
            ObligacionesDalc ObligD = new ObligacionesDalc(conn);

            Ds = ObligD.CargarObligacionesDelLote(sLote);

            Errores = ObligD.Errores;
            return Ds;

        }

        #endregion

        #region Carga Masiva

        private bool CargaMasiva()
        {
            ObligacionesDalc oblgD = new ObligacionesDalc(conn);

            //1) De las obligaciones que hay en produccón copiaremos los datos que interesa mantener
            oblgD.CopiarDatosProductivos();

            if (oblgD.Errores.Cantidad() > 0)
            {
                this.Errores.Agregar(oblgD.Errores);
                cIncidencia.Generar(oblgD.Errores, "CopiarDatosProductivos", "Error al actualizar datos productivos a tabla IN");
                return false;
            }

            /// 2) Se informara el campo TipoOper: 
            /// A - Alta: La oblig no existe ni en ROBLG ni en RHISTO. 
            /// M - La obligación existe en ROBLG
            /// R -reentrada La oblig NO existe en ROBLG pero si en RHISTO
            oblgD.MarcarAltas();

            if (oblgD.Errores.Cantidad() > 0)
            {
                cIncidencia.Generar(oblgD.Errores, "MarcarAltas", "Error al marcar el tipo de operación - Alta");
                return false;
            }

            oblgD.MarcarModificaciones();

            if (oblgD.Errores.Cantidad() > 0)
            {
                cIncidencia.Generar(oblgD.Errores, "MarcarModificaciones", "Error al marcar el tipo de operación - Modificacion");
                return false;
            }

            oblgD.MarcarReentradas();

            if (oblgD.Errores.Cantidad() > 0)
            {
                cIncidencia.Generar(oblgD.Errores, "MarcarReentradas", "Error al marcar el tipo de operación - Reentrada");
                return false;
            }

            //3) Se borran las obligs (ROBLG) que lleguen en la IN
            oblgD.BorrarObligaciones();

            if (oblgD.Errores.Cantidad() > 0)
            {
                cIncidencia.Generar(oblgD.Errores, "BorrarObligaciones", "Error al borrar obligaciones");
                return false;
            }

            //4) Insertamos insert-select de obligs y de RHISTO
            oblgD.InsertarObligaciones();

            if (oblgD.Errores.Cantidad() > 0)
            {
                cIncidencia.Generar(oblgD.Errores, "InsertarObligaciones", "Error insertar las obligaciones");
                return false;
            }

            oblgD.CrearHistoricos();

            if (oblgD.Errores.Cantidad() > 0)
            {
                cIncidencia.Generar(oblgD.Errores, "CrearHistoricos", "Error insertar las Históricos");
                return false;
            }

            //5) Según el tipo de opración se registran los TAR's correspondientes
            oblgD.CrearGestionesAltaReentrada();

            if (oblgD.Errores.Cantidad() > 0)
            {
                cIncidencia.Generar(oblgD.Errores, "InsertarObligaciones", "Error insertar crear Gestiones");
                return false;
            }

            return true;
        }

        #endregion
    }
}

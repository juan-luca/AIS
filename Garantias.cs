using BusComun;
using Comun;
using System;
using System.Collections.Generic;
using System.Threading;


namespace BusInchost
{
    public class Garantias : cBase
    {
        public Garantias(cConexion pconn)
        {
            conn = pconn;
        }

        public bool ProcesoGarantias()
        {
            return CargaMasiva();
        }

        #region Caga Unitaria

        /// <summary>
        /// Carga unitaria de Garantías
        /// </summary>
        private bool CargaUitaria()
        {
            try
            {
                long nContTrat = 0;
                long nContOK = 0;

                string sClienteAnt = "";

                GarantiasDalc GarantiasD = new GarantiasDalc(conn);

                TyGarantia Garantia = new TyGarantia();
                List<TyGarantia> GarantiasCliente = new List<TyGarantia>();

                AISDataReader DrT = new AISDataReader();

                // Uso una segunda conexion a la base para el DataReader Principal
                cConexion connGarantias = new cConexion(cGlobales.XMLConex);
                connGarantias.Conectar();
                if (connGarantias.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(connGarantias.Errores, "OPENBDGARA", "No se pudo abrir la conexion");
                }

                //Abro el cursor de Garantias recibidos en la tabla de intercambio
                GarantiasDalc vGarantiaBD = new GarantiasDalc(connGarantias);
                DrT = vGarantiaBD.AbrirCursorNuevosGarantias();

                if (vGarantiaBD.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(vGarantiaBD.Errores, "OPENGARA", "Error al abrir cursor de Garantias");
                    return false;
                }

                // Establezco la cantidad de hilos que se van a utilizar y los creo                 
                Hilos HilosGarantia = new Hilos(cGlobales.nGarantiasXHilo, cGlobales.nHilosGarantias);

                // Conecto los hilos a la base de datos
                if (!HilosGarantia.ConectarHilosABaseDeDatos(cGlobales.XMLConex))
                    return false;

                /* Cargo en cada elemento del hilo todos los Garantias de cada cliente*/
                while (vGarantiaBD.FechGarantia(DrT, ref Garantia))
                {
                    nContTrat++;

                    if (sClienteAnt != Garantia.GACOD)
                    {
                        sClienteAnt = Garantia.GACOD;
                        if (GarantiasCliente.Count > 0)
                        {
                            if (!HilosGarantia.CargoElementoEnAlgunHilo(GarantiasCliente))
                                HilosGarantia.Arrancar(new ParameterizedThreadStart(FuncionArranqueHilo));
                        }
                        GarantiasCliente = new List<TyGarantia>();
                    }
                    GarantiasCliente.Add(Garantia);
                    cIncidencia.SetMarcaVelocidad(nContTrat);

                }
                /* Cargo los Garantias del ultimo cliente*/
                if (GarantiasCliente.Count > 0)
                    HilosGarantia.CargoElementoEnAlgunHilo(GarantiasCliente);

                HilosGarantia.Arrancar(new ParameterizedThreadStart(FuncionArranqueHilo));

                DrT.Close();
                connGarantias.Desconectar();

                if (vGarantiaBD.Errores.Cantidad() != 0)
                    return false;
                else
                {
                    if (HilosGarantia.EjecucionDeLosHilosOk())
                    {
                        nContTrat = HilosGarantia.ElemenosTratados();
                        nContOK = HilosGarantia.ElemenosTratadosOk();

                        HilosGarantia.CommitDeLosHilos();
                        HilosGarantia.DesConectarHilosABaseDeDatos();
                        cIncidencia.Aviso("Garantias: Tratados -> " + nContTrat.ToString() + " OK -> " + nContOK);
                        return true;
                    }
                    else
                    {
                        HilosGarantia.RollbackDeLosHilos();
                        HilosGarantia.DesConectarHilosABaseDeDatos();
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
            Garantias GarantiasActual;
            HiloAIS Hilo;


            Hilo = (HiloAIS)obj;
            GarantiasActual = new Garantias(Hilo.Conexion);

            GarantiasActual.TratarListaDeGarantiasDeClientes(obj);

            return;
        }

        /*  obj es una lista de cliente, donde para cada cliente hay una lista de Garantias*/
        private void TratarListaDeGarantiasDeClientes(object obj)
        {
            HiloAIS Hilo;
            string sIncidencia = "";


            Hilo = (HiloAIS)obj;

            List<object> ListaClientes = Hilo.ListaElementosAIS;

            foreach (List<TyGarantia> ListaGarantias in ListaClientes)
            {
                foreach (TyGarantia Garantia in ListaGarantias)
                {
                    Hilo.nContTratados++;
                    if (TrataGarantia(Garantia))
                        Hilo.nContOk++;
                    else
                    {
                        sIncidencia = "GARA" + "|" + Garantia.GACOD;
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

            return;
        }

        private bool TrataGarantia(TyGarantia Garantia)
        {
            return TratoAltaModifGARA(Garantia);
        }

        private bool TratoAltaModifGARA(TyGarantia Garantia)
        {
            bool retorno = false;
            GarantiasDalc GarDalc = new GarantiasDalc(conn);

            TyGarantia BDGarantia = new TyGarantia
            {
                GACOD = Garantia.GACOD,
                GAOPERACION = Garantia.GAOPERACION,
                GATIPO = Garantia.GATIPO,
                GANUMERO = Garantia.GANUMERO
            };

            TipoAccion nRes = GarDalc.ObtengoGarantiaBD(BDGarantia);

            switch (nRes)
            {
                case TipoAccion.Alta:
                    retorno = GarDalc.InsertaGarantiaBD(Garantia);
                    break;
                case TipoAccion.Modificacion:
                    if (HayCambiosElContenidoDelRegistro(Garantia, BDGarantia))
                    {
                        retorno = GarDalc.ModificaGarantiaBD(Garantia);
                    }
                    break;

                default:
                    retorno = false;
                    break;
            }

            return retorno;
        }

        private bool HayCambiosElContenidoDelRegistro(TyGarantia Garantia, TyGarantia BDGarantia)
        {
            bool nRet = false;

            nRet = nRet || (Garantia.GAMONEDA != BDGarantia.GAMONEDA);
            nRet = nRet || (Garantia.GAMONTO != BDGarantia.GAMONTO);
            nRet = nRet || (Garantia.GAVALOR != BDGarantia.GAVALOR);
            nRet = nRet || (Garantia.GAFECVTO != BDGarantia.GAFECVTO);

            return nRet;
        }

        #endregion

        #region Carga Masiva

        /// <summary>
        /// Carga masiva de Garantías
        /// </summary>
        private bool CargaMasiva()
        {
            GarantiasDalc GarantiasD = new GarantiasDalc(conn);

            GarantiasD.BorrarGarantias();

            if (GarantiasD.Errores.Cantidad() > 0)
            {
                cIncidencia.Generar(Errores, "BorrarGarantias", "Error al borrar Garantias");
                return false;
            }

            GarantiasD.InsertaGarantia();
            if (GarantiasD.Errores.Cantidad() > 0)
            {
                cIncidencia.Generar(Errores, "InsertaGarantia", "Error al Insertar Garantias");
                return false;
            }

            return true;
        }

        #endregion

    }
}

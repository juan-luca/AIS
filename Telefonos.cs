using BusComun;
using Comun;
using System;
using System.Collections.Generic;
using System.Threading;


namespace BusInchost
{
    public class Telefonos : cBase
    {
        public Telefonos(cConexion pconn)
        {
            conn = pconn;
        }

        public bool ProcesoTelefonos()
        {
            return ProcesoMasivo();
        }

        #region Proceso unitario (1 a 1)

        /// <summary>
        /// Proceso unitario de Teléfonos (1 a 1)
        /// </summary>
        /// <returns></returns>
        private bool ProcesoUnitario()
        {
            try
            {
                long nContTrat = 0;
                long nContOK = 0;

                string sClienteAnt = "";

                TelefonosDalc TelefonosD = new TelefonosDalc(conn);

                TyTelefono Telefono = new TyTelefono();
                List<TyTelefono> TelefonosCliente = new List<TyTelefono>();

                AISDataReader DrT = new AISDataReader();

                // Uso una segunda conexion a la base para el DataReader Principal
                cConexion connTelefonos = new cConexion(cGlobales.XMLConex);
                connTelefonos.Conectar();
                if (connTelefonos.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(connTelefonos.Errores, "OPENBDTELE", "No se pudo abrir la conexion");
                }

                //Abro el cursor de Telefonos recibidos en la tabla de intercambio
                TelefonosDalc vTelefonoBD = new TelefonosDalc(connTelefonos);
                DrT = vTelefonoBD.AbrirCursorNuevosTelefonos();

                if (vTelefonoBD.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(vTelefonoBD.Errores, "OPENTELE", "Error al abrir cursor de Telefonos");
                    return false;
                }

                // Establezco la cantidad de hilos que se van a utilizar y los creo 
                Hilos HilosTelefono = new Hilos(cGlobales.nTelefonosXHilo, cGlobales.nHilosTelefonos);

                // Conecto los hilos a la base de datos
                if (!HilosTelefono.ConectarHilosABaseDeDatos(cGlobales.XMLConex))
                    return false;

                /* Cargo en cada elemento del hilo todos los telefonos de cada cliente*/
                while (vTelefonoBD.FechTelefono(DrT, ref Telefono))
                {
                    nContTrat++;

                    if (sClienteAnt != Telefono.TECOD)
                    {
                        sClienteAnt = Telefono.TECOD;
                        if (TelefonosCliente.Count > 0)
                        {
                            if (!HilosTelefono.CargoElementoEnAlgunHilo(TelefonosCliente))
                                HilosTelefono.Arrancar(new ParameterizedThreadStart(FuncionArranqueHilo));
                        }
                        TelefonosCliente = new List<TyTelefono>();
                    }
                    TelefonosCliente.Add(Telefono);
                    cIncidencia.SetMarcaVelocidad(nContTrat);

                }
                /* Cargo los telefonos del ultimo cliente*/
                if (TelefonosCliente.Count > 0)
                    HilosTelefono.CargoElementoEnAlgunHilo(TelefonosCliente);

                HilosTelefono.Arrancar(new ParameterizedThreadStart(FuncionArranqueHilo));

                DrT.Close();
                connTelefonos.Desconectar();

                if (vTelefonoBD.Errores.Cantidad() != 0)
                    return false;
                else
                {
                    if (HilosTelefono.EjecucionDeLosHilosOk())
                    {
                        nContTrat = HilosTelefono.ElemenosTratados();
                        nContOK = HilosTelefono.ElemenosTratadosOk();

                        HilosTelefono.CommitDeLosHilos();
                        HilosTelefono.DesConectarHilosABaseDeDatos();
                        cIncidencia.Aviso("Telefonos: Tratados -> " + nContTrat.ToString() + " OK -> " + nContOK);
                        return true;
                    }
                    else
                    {
                        HilosTelefono.RollbackDeLosHilos();
                        HilosTelefono.DesConectarHilosABaseDeDatos();
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
            Telefonos TelefonosActual;
            HiloAIS Hilo;

            Hilo = (HiloAIS)obj;
            TelefonosActual = new Telefonos(Hilo.Conexion);

            TelefonosActual.TratarListaDeTelefonosDeClientes(obj);

            return;
        }

        /*  obj es una lista de cliente, donde para cada cliente hay una lista de telefonos*/
        private void TratarListaDeTelefonosDeClientes(object obj)
        {
            HiloAIS Hilo;
            string sIncidencia = "";

            Hilo = (HiloAIS)obj;

            List<object> ListaClientes = Hilo.ListaElementosAIS;

            foreach (List<TyTelefono> ListaTelefonos in ListaClientes)
            {
                foreach (TyTelefono Telefono in ListaTelefonos)
                {
                    Hilo.nContTratados++;
                    if (TrataTelefono(Telefono))
                        Hilo.nContOk++;
                    else
                    {
                        sIncidencia = "TELE" + "|" + Telefono.TECOD;
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

        private bool TrataTelefono(TyTelefono Telefono)
        {
            return TratoAltaModifTele(Telefono);
        }

        private bool TratoAltaModifTele(TyTelefono Telefono)
        {
            bool result = true;

            TelefonosDalc TelDalc = new TelefonosDalc(conn);
            TyTelefono BDTelefono = new TyTelefono
            {
                TECOD = Telefono.TECOD,
                TEORIGEN = Telefono.TEORIGEN,
                TECODTEL = Telefono.TECODTEL,
                TETIPTEL = Telefono.TETIPTEL
            };

            TipoAccion nRes = TelDalc.ObtengoTelefonoBD(BDTelefono);

            switch (nRes)
            {
                case TipoAccion.Alta:
                    result = TelDalc.InsertaTelefonoBD(Telefono);
                    break;

                case TipoAccion.Modificacion:
                    if (HayCambiosElContenidoDelRegistro(Telefono, BDTelefono))
                    {
                        result = TelDalc.ModificaTelefonoBD(Telefono);
                    }
                    break;

                default:
                    result = false;
                    break;
            }

            return result;
        }

        private bool HayCambiosElContenidoDelRegistro(TyTelefono Telefono, TyTelefono BDTelefono)
        {
            bool nRet = false;

            nRet = nRet || (Telefono.TETIPTEL != BDTelefono.TETIPTEL);
            nRet = nRet || (Telefono.TENUMERO != BDTelefono.TENUMERO);
            nRet = nRet || (Telefono.TEPRIORIDAD != BDTelefono.TEPRIORIDAD);
            nRet = nRet || (Telefono.TEDEFAULT != BDTelefono.TEDEFAULT);
            nRet = nRet || (Telefono.TEOBS != BDTelefono.TEOBS);
            nRet = nRet || (Telefono.TEFECBAJA != BDTelefono.TEFECBAJA);
            nRet = nRet || (Telefono.TECODPAIS != BDTelefono.TECODPAIS);
            nRet = nRet || (Telefono.TECODAREA != BDTelefono.TECODAREA);
            nRet = nRet || (Telefono.TEVALIDO != BDTelefono.TEVALIDO);

            return nRet;
        }

        #endregion

        #region Proceso Masivo

        /// <summary>
        /// Carga masiva de Teléfonos (delte + insert-select)
        /// </summary>
        private bool ProcesoMasivo()
        {
            TelefonosDalc TelefonosD = new TelefonosDalc(conn);

            TelefonosD.BorradoTelefonos();

            if (TelefonosD.Errores.Cantidad() > 0)
            {
                cIncidencia.Generar(TelefonosD.Errores, "BorradoTelefonos", "Error al Borrar Teléfonos ");
                return false;
            }

            TelefonosD.InsertTelefonos();

            if (TelefonosD.Errores.Cantidad() > 0)
            {
                cIncidencia.Generar(TelefonosD.Errores, "InsertTelefonos", "Error al Borrar Teléfonos ");
                return false;
            }

            return true;
        }

        #endregion

    }
}

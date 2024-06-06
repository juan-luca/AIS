using BusComun;
using Comun;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;

namespace BusInchost
{
    public class Clientes : cBase
    {

        public Clientes(cConexion pconn)
        {
            conn = pconn;
        }

        public bool ProcesoClientes()
        {
            return ProcesoMasivo();
            //return ProcesoUinitario();
        }

        #region Tratamietno Masivo

        internal bool ProcesoMasivo()
        {
            bool ret = true;
            ClientesDalc ClientesD = new ClientesDalc(conn);

            // 1) Borrado de clietnes 
            ret = ClientesD.BorradoClientes();

            if (!ret || ClientesD.Errores.Cantidad() > 0)
            {
                this.Errores.Agregar(ClientesD.Errores);
                return false;
            }

            // 2) Inserción de clientes
            ret = ClientesD.AltaClientes();

            if (!ret || ClientesD.Errores.Cantidad() > 0)
            {
                this.Errores.Agregar(ClientesD.Errores);
                return false;
            }

            return ret;
        }

        #endregion

        #region Tratamiento individual

        private bool ProcesoUinitario()
        {
            try
            {
                long nContTrat = 0;
                long nContOK = 0;

                HistoricosDalc Depurador = new HistoricosDalc(conn);
                if (!Depurador.ClienteActualizarMarcaEstado())
                    return false;

                conn.CommitTransaccion();
                conn.ComienzoTransaccion();

                ClientesDalc ClientesD = new ClientesDalc(conn);

                /* Trato primero las bajas de clientes */
                //if (!ClientesD.BajasDeClientes())
                //    return false;

                //conn.CommitTransaccion();
                //conn.ComienzoTransaccion();

                TyCliente Cliente = new TyCliente();

                AISDataReader DrC = new AISDataReader();

                // Uso una segunda conexion a la base para el DataReader Principal
                cConexion connClientes = new cConexion(cGlobales.XMLConex);
                connClientes.Conectar();

                if (connClientes.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(connClientes.Errores, "OPENBDCLIE", "No se pudo abrir la conexion");
                    return false;
                }

                //Abro el cursor de Clientes recibidos en la tabla de intercambio
                ClientesDalc vClienteBD = new ClientesDalc(connClientes);
                DrC = vClienteBD.AbrirCursorNuevosClientes();

                if (vClienteBD.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(vClienteBD.Errores, "OPENCLIE", "Error al abrir cursor de Clientes");
                    return false;
                }

                // Establezco la cantidad de hilos que se van a utilizar y los creo 
                Hilos HilosCliente = new Hilos(cGlobales.nClientesXHilo, cGlobales.nHilosClientes);

                // Conecto los hilos a la base de datos
                if (!HilosCliente.ConectarHilosABaseDeDatos(cGlobales.XMLConex))
                    return false;


                while (vClienteBD.FechCliente(DrC, ref Cliente))
                {

                    nContTrat++;
                    // Cargo el elemento en alguno de los hilos disponible
                    // Si la CargoElementoEnAlgunHilo retorna falso entonces indica que los hilos completos 
                    // y listos para ejecutar
                    if (!HilosCliente.CargoElementoEnAlgunHilo(Cliente))
                        HilosCliente.Arrancar(new ParameterizedThreadStart(FuncionArranqueHilo));

                    cIncidencia.SetMarcaVelocidad(nContTrat);
                }

                HilosCliente.Arrancar(new ParameterizedThreadStart(FuncionArranqueHilo));

                DrC.Close();
                connClientes.Desconectar();

                if (vClienteBD.Errores.Cantidad() != 0)
                    return false;
                else
                {
                    if (HilosCliente.EjecucionDeLosHilosOk())
                    {
                        nContTrat = HilosCliente.ElemenosTratados();
                        nContOK = HilosCliente.ElemenosTratadosOk();

                        HilosCliente.CommitDeLosHilos();
                        HilosCliente.DesConectarHilosABaseDeDatos();
                        cIncidencia.Aviso("Clientes: Tratados -> " + nContTrat.ToString() + " OK -> " + nContOK);
                        return true;
                    }
                    else
                    {
                        HilosCliente.RollbackDeLosHilos();
                        HilosCliente.DesConectarHilosABaseDeDatos();
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

        private void FuncionArranqueHilo(object obj)
        {
            Clientes ClienteActual;
            HiloAIS Hilo;


            Hilo = (HiloAIS)obj;
            ClienteActual = new Clientes(Hilo.Conexion);

            ClienteActual.TratarClienteLista(obj);

            return;
        }

        private void TratarClienteLista(object obj)
        {
            HiloAIS Hilo;
            string sIncidencia = "";


            Hilo = (HiloAIS)obj;

            List<object> ListaClientes = Hilo.ListaElementosAIS;

            foreach (TyCliente Cliente in ListaClientes)
            {
                Hilo.nContTratados++;
                if (TrataCliente(Cliente))
                    Hilo.nContOk++;
                else
                {
                    sIncidencia = "CLIE" + "|" + Cliente.CLCOD;
                    cIncidencia.IncidenciaInterfaces(cGlobales.FicheroIncid, sIncidencia);
                }
                if ((Hilo.nContTratados % cGlobales.nFrecuenciaCommit) == 0)
                {
                    conn.CommitTransaccion();
                    conn.ComienzoTransaccion();
                }
            }
            Hilo.bEjecutadoOk = (Hilo.bEjecutadoOk && true);

            return;
        }

        private bool TrataCliente(TyCliente Cliente)
        {
            ClientesDalc ClientesD = new ClientesDalc(conn);

            if (!ClientesD.ModificarCliente(Cliente))
            {
                if (!ClientesD.InsertarCliente(Cliente))
                    return false;
            }

            return true;
        }

        #endregion

        internal DataSet CargarClientesDelLote(string sLote)
        {
            DataSet Ds = new DataSet();
            ClientesDalc ClieD = new ClientesDalc(conn);

            Ds = ClieD.CargarClientesDelLote(sLote);

            Errores = ClieD.Errores;
            return Ds;
        }

        #region Documentos Cliente

        public bool ProcesoDocumentosClie()
        {
            ClientesDalc ClieD = new ClientesDalc(conn);
            bool ret = false;

            if (ClieD.VaciarDocumentos())
                ret = ClieD.CargarDocumentos();

            return ret;
        }

        #endregion

        #region Atributos cliente

        public bool ProcesoAtributosClientes()
        {
            ClientesDalc ClieD = new ClientesDalc(conn);
            bool ret = false;

            if (ClieD.VaciarAtributos())
                ret = ClieD.CargarAtributos();

            return ret;
        }

        #endregion
    }
}

using BusComun;
using Comun;
using System;
using System.Collections.Generic;
using System.Threading;

namespace BusInchost
{
    public class Direcciones : cBase
    {
        public Direcciones(cConexion pconn)
        {
            conn = pconn;
        }

        public bool ProcesoDirecciones()
        {
            return CargaMasiva();
        }

        #region Carga unitaria

        private bool ProcesoUnitario()
        {
            try
            {
                long nContTrat = 0;
                long nContOK = 0;

                string sClienteAnt = "";

                DireccionesDalc DireccionesD = new DireccionesDalc(conn);

                TyDireccion Direccion = new TyDireccion();
                List<TyDireccion> DireccionesCliente = new List<TyDireccion>();

                AISDataReader DrT = new AISDataReader();

                // Uso una segunda conexion a la base para el DataReader Principal
                cConexion connDirecciones = new cConexion(cGlobales.XMLConex);
                connDirecciones.Conectar();

                if (connDirecciones.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(connDirecciones.Errores, "OPENBDDIRE", "No se pudo abrir la conexion");
                }

                //Abro el cursor de Direcciones recibidos en la tabla de intercambio
                DireccionesDalc vDireccionBD = new DireccionesDalc(connDirecciones);
                DrT = vDireccionBD.AbrirCursorNuevosDirecciones();

                if (vDireccionBD.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(vDireccionBD.Errores, "OPENDIRE", "Error al abrir cursor de Direcciones");
                    return false;
                }

                // Establezco la cantidad de hilos que se van a utilizar y los creo 
                Hilos HilosDireccion = new Hilos(cGlobales.nDireccionesXHilo, cGlobales.nHilosDirecciones);

                // Conecto los hilos a la base de datos
                if (!HilosDireccion.ConectarHilosABaseDeDatos(cGlobales.XMLConex))
                    return false;

                /* Cargo en cada elemento del hilo todos los Direcciones de cada cliente*/
                while (vDireccionBD.FechDireccion(DrT, ref Direccion))
                {
                    nContTrat++;

                    if (sClienteAnt != Direccion.DTCOD)
                    {
                        sClienteAnt = Direccion.DTCOD;
                        if (DireccionesCliente.Count > 0)
                        {
                            if (!HilosDireccion.CargoElementoEnAlgunHilo(DireccionesCliente))
                                HilosDireccion.Arrancar(new ParameterizedThreadStart(FuncionArranqueHilo));
                        }
                        DireccionesCliente = new List<TyDireccion>();
                    }
                    DireccionesCliente.Add(Direccion);

                    cIncidencia.SetMarcaVelocidad(nContTrat);
                }
                /* Cargo los Direcciones del ultimo cliente*/
                if (DireccionesCliente.Count > 0)
                    HilosDireccion.CargoElementoEnAlgunHilo(DireccionesCliente);

                HilosDireccion.Arrancar(new ParameterizedThreadStart(FuncionArranqueHilo));

                DrT.Close();
                connDirecciones.Desconectar();

                if (vDireccionBD.Errores.Cantidad() != 0)
                    return false;
                else
                {
                    if (HilosDireccion.EjecucionDeLosHilosOk())
                    {
                        nContTrat = HilosDireccion.ElemenosTratados();
                        nContOK = HilosDireccion.ElemenosTratadosOk();

                        HilosDireccion.CommitDeLosHilos();
                        HilosDireccion.DesConectarHilosABaseDeDatos();
                        cIncidencia.Aviso("Direcciones: Tratados -> " + nContTrat.ToString() + " OK -> " + nContOK);
                        return true;
                    }
                    else
                    {
                        HilosDireccion.RollbackDeLosHilos();
                        HilosDireccion.DesConectarHilosABaseDeDatos();
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
            Direcciones DireccionesActual;
            HiloAIS Hilo;


            Hilo = (HiloAIS)obj;
            DireccionesActual = new Direcciones(Hilo.Conexion);

            DireccionesActual.TratarListaDeDireccionesDeClientes(obj);

            return;
        }

        private void TratarListaDeDireccionesDeClientes(object obj)
        {
            HiloAIS Hilo;
            string sIncidencia = "";


            Hilo = (HiloAIS)obj;

            List<object> ListaClientes = Hilo.ListaElementosAIS;

            foreach (List<TyDireccion> ListaDirecciones in ListaClientes)
            {
                foreach (TyDireccion Direccion in ListaDirecciones)
                {
                    Hilo.nContTratados++;
                    if (TrataDireccion(Direccion))
                        Hilo.nContOk++;
                    else
                    {
                        sIncidencia = "DIRE" + "|" + Direccion.DTCOD;
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

        private bool TrataDireccion(TyDireccion Direccion)
        {
            return TratoAltaModifDire(Direccion);
        }

        private bool TratoAltaModifDire(TyDireccion Direccion)
        {
            bool res = false;

            DireccionesDalc DomDalc = new DireccionesDalc(conn);

            TyDireccion BDDireccion = new TyDireccion
            {
                DTCOD = Direccion.DTCOD,
                DTCODDOM = Direccion.DTCODDOM,
                DTTIPDOM = Direccion.DTTIPDOM,
                DTORIGEN = Direccion.DTORIGEN
            };

            TipoAccion Tipo = DomDalc.ObtengoDireccionBD(BDDireccion);

            switch (Tipo)
            {
                case TipoAccion.Alta:
                    res = DomDalc.InsertaDireccionBD(Direccion);
                    break;

                case TipoAccion.Modificacion:
                    // Si hay cambios en los datos del Direccion entonces actualizo el registro
                    if (HayCambiosElContenidoDelRegistro(Direccion, BDDireccion))
                    {
                        if (DomDalc.ModificaDireccionBD(Direccion))
                            res = true;
                    }
                    break;
                default: // Si hubo un error
                    res = false;
                    break;
            }

            return res;
        }

        private bool HayCambiosElContenidoDelRegistro(TyDireccion Direccion, TyDireccion BDDireccion)
        {
            bool nRet = false;

            nRet = nRet || (Direccion.DTDEPTO != BDDireccion.DTDEPTO);
            nRet = nRet || (Direccion.DTOBSERVACIONES != BDDireccion.DTOBSERVACIONES);
            nRet = nRet || (Direccion.DTVALIDO != BDDireccion.DTVALIDO);
            nRet = nRet || (Direccion.DTDEFECTO != BDDireccion.DTDEFECTO);
            nRet = nRet || (Direccion.DTFECBAJA != BDDireccion.DTFECBAJA);
            nRet = nRet || (Direccion.DTLOCALIDAD != BDDireccion.DTLOCALIDAD);
            nRet = nRet || (Direccion.DTCALLE != BDDireccion.DTCALLE);
            nRet = nRet || (Direccion.DTPROVINCIA != BDDireccion.DTPROVINCIA);
            nRet = nRet || (Direccion.DTCODPOS != BDDireccion.DTCODPOS);
            nRet = nRet || (Direccion.DTPAIS != BDDireccion.DTPAIS);
            nRet = nRet || (Direccion.DTNUMERO != BDDireccion.DTNUMERO);
            nRet = nRet || (Direccion.DTPISO != BDDireccion.DTPISO);
            nRet = nRet || (Direccion.DTTIPDOM != BDDireccion.DTTIPDOM);

            return nRet;
        }

        #endregion

        #region Carga Masiva

        private bool CargaMasiva()
        {
            DireccionesDalc DireccionesD = new DireccionesDalc(conn);

            DireccionesD.BorrarDirecciones();

            if (DireccionesD.Errores.Cantidad() > 0)
            {
                cIncidencia.Generar(DireccionesD.Errores, "BorrarDirecciones", "Error al Borrar Direcciones");
                return false;
            }

            DireccionesD.InsertareDirecciones();

            if (DireccionesD.Errores.Cantidad() > 0)
            {
                cIncidencia.Generar(DireccionesD.Errores, "InsertareDirecciones", "Error al Insertar Direcciones");
                return false;
            }

            return true;
        }

        #endregion

    }
}

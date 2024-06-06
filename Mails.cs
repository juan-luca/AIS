using BusComun;
using Comun;
using System;
using System.Collections.Generic;
using System.Threading;

namespace BusInchost
{
    public class Mails : cBase
    {
        public Mails(cConexion pconn)
        {
            conn = pconn;
        }

        /// <summary>
        /// Proceso de carga de eMails de los clientes
        /// </summary>
        public bool ProcesoMails()
        {
            return ProcesoMasivo();
        }

        #region Proceso unitario

        /// <summary>
        /// Proceso unitario de eMails (insert 1 a 1)
        /// </summary>
        public bool ProcesoUnitario()
        {
            try
            {
                long nContTrat = 0;
                long nContOK = 0;

                string sClienteAnt = "";


                MailsDalc MailsD = new MailsDalc(conn);

                TyMail Mail = new TyMail();
                List<TyMail> MailsCliente = new List<TyMail>();

                AISDataReader DrT = new AISDataReader();

                // Uso una segunda conexion a la base para el DataReader Principal
                cConexion connMails = new cConexion(cGlobales.XMLConex);
                connMails.Conectar();
                if (connMails.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(connMails.Errores, "OPENBDMAIL", "No se pudo abrir la conexion");
                }

                //Abro el cursor de Mails recibidos en la tabla de intercambio
                MailsDalc vMailBD = new MailsDalc(connMails);
                DrT = vMailBD.AbrirCursorNuevosMails();

                if (vMailBD.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(vMailBD.Errores, "OPENMAIL", "Error al abrir cursor de Mails");
                    return false;
                }

                // Establezco la cantidad de hilos que se van a utilizar y los creo 
                Hilos HilosMail = new Hilos(cGlobales.nMailsXHilo, cGlobales.nHilosMails);

                // Conecto los hilos a la base de datos
                if (!HilosMail.ConectarHilosABaseDeDatos(cGlobales.XMLConex))
                    return false;

                /* Cargo en cada elemento del hilo todos los Mails de cada cliente*/
                while (vMailBD.FechMail(DrT, ref Mail))
                {
                    nContTrat++;

                    if (sClienteAnt != Mail.EMCOD)
                    {
                        sClienteAnt = Mail.EMCOD;
                        if (MailsCliente.Count > 0)
                        {
                            if (!HilosMail.CargoElementoEnAlgunHilo(MailsCliente))
                                HilosMail.Arrancar(new ParameterizedThreadStart(FuncionArranqueHilo));
                        }
                        MailsCliente = new List<TyMail>();
                    }
                    MailsCliente.Add(Mail);
                    cIncidencia.SetMarcaVelocidad(nContTrat);
                }
                /* Cargo los Mails del ultimo cliente*/
                if (MailsCliente.Count > 0)
                    HilosMail.CargoElementoEnAlgunHilo(MailsCliente);

                HilosMail.Arrancar(new ParameterizedThreadStart(FuncionArranqueHilo));

                DrT.Close();
                connMails.Desconectar();

                if (vMailBD.Errores.Cantidad() != 0)
                    return false;
                else
                {
                    if (HilosMail.EjecucionDeLosHilosOk())
                    {
                        nContTrat = HilosMail.ElemenosTratados();
                        nContOK = HilosMail.ElemenosTratadosOk();

                        HilosMail.CommitDeLosHilos();
                        HilosMail.DesConectarHilosABaseDeDatos();
                        cIncidencia.Aviso("Mails: Tratados -> " + nContTrat.ToString() + " OK -> " + nContOK);
                        return true;
                    }
                    else
                    {
                        HilosMail.RollbackDeLosHilos();
                        HilosMail.DesConectarHilosABaseDeDatos();
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
            Mails MailsActual;
            HiloAIS Hilo;


            Hilo = (HiloAIS)obj;
            MailsActual = new Mails(Hilo.Conexion);

            MailsActual.TratarListaDeMailsDeClientes(obj);

            return;
        }

        private void TratarListaDeMailsDeClientes(object obj)
        {
            HiloAIS Hilo;
            string sIncidencia = "";


            Hilo = (HiloAIS)obj;

            List<object> ListaClientes = Hilo.ListaElementosAIS;

            foreach (List<TyMail> ListaMails in ListaClientes)
            {
                foreach (TyMail Mail in ListaMails)
                {
                    Hilo.nContTratados++;
                    if (TrataMail(Mail))
                        Hilo.nContOk++;
                    else
                    {
                        sIncidencia = "MAIL" + "|" + Mail.EMCOD;
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

        private bool TrataMail(TyMail Mail)
        {
            return TratoAltaModifMail(Mail);
        }

        private bool TratoAltaModifMail(TyMail Mail)
        {
            bool retorno = true;

            MailsDalc TelDalc = new MailsDalc(conn);

            TyMail BDMail = new TyMail
            {
                EMCOD = Mail.EMCOD,
                EMCODMAIL = Mail.EMCODMAIL,
                EMORIGEN = Mail.EMORIGEN
            };

            TipoAccion nRes = TelDalc.ObtengoMailBD(BDMail);

            switch (nRes)
            {
                case TipoAccion.Alta:
                    retorno = TelDalc.InsertaMailBD(Mail);
                    break;
                case TipoAccion.Modificacion:
                    if (HayCambiosElContenidoDelRegistro(Mail, BDMail))
                        retorno = TelDalc.ModificaMailBD(Mail);
                    break;
                default:
                    retorno = false;
                    break;
            }

            return retorno;
        }

        private bool HayCambiosElContenidoDelRegistro(TyMail Mail, TyMail BDMail)
        {
            bool nRet = false;

            nRet = nRet || (Mail.EMMAIL != BDMail.EMMAIL);

            return nRet;
        }

        #endregion

        #region Proceso Masivo

        /// <summary>
        /// Proceso masivo de eMails (borrado masivo e Insert-Select
        /// </summary>
        private bool ProcesoMasivo()
        {
            MailsDalc MailsD = new MailsDalc(conn);

            MailsD.BorrareMails();

            if (MailsD.Errores.Cantidad() > 0)
            {
                cIncidencia.Generar(MailsD.Errores, "BorrarDirecciones", "Error al Borrar eMails");
                return false;
            }

            MailsD.InsertareMails();

            if (MailsD.Errores.Cantidad() > 0)
            {
                cIncidencia.Generar(MailsD.Errores, "InsertareMails", "Error al Insertar eMails");
                return false;
            }

            return true;
        }

        #endregion

    }
}
